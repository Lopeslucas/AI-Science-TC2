"""Contratos dinâmicos Bronze/Silver; usa fontes simuladas, sem acessar AWS."""
import contextlib
import importlib
from io import BytesIO, StringIO
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src' / 'transformation'))


class SilverContractsTests(unittest.TestCase):
    def fixture(self, entity, year=2032, horizon=(2035, 2037)):
        data = {'ano': [year], 'rede': ['Municipal' if entity == 'municipio' else 'Pública'],
                'taxa_alfabetizacao': [60.], 'percentual_participacao': [90.]}
        if entity == 'municipio':
            data.update(id_municipio=['1234567'], nivel_alfabetizacao=['2'])
        elif entity == 'uf':
            data.update(sigla_uf=['SP'])
        for index, target_year in enumerate(horizon):
            data[f'meta_alfabetizacao_{target_year}'] = [None if index == 0 else 80.]
        transform = importlib.import_module(f'transform_meta_{entity}_silver')
        validator = importlib.import_module(f'validate_meta_{entity}_silver')
        frame = pd.DataFrame(data)
        transform.validate_bronze_schema(frame)
        return transform, validator, transform.standardize_types(frame)

    def validate_silver(self, validator, frame):
        validator.validate_schema(frame)
        validator.validate_ano_meta(frame)
        validator.validate_ranges(frame)
        validator.validate_natural_key(frame)
        if hasattr(validator, 'validate_id_municipio'):
            validator.validate_id_municipio(frame)
        if hasattr(validator, 'validate_ufs'):
            validator.validate_ufs(frame)

    def reconcile(self, transform, validator, bronze, gold):
        with patch.object(transform, 'list_parquet_files', return_value=[f'bronze/ano={int(bronze.ano.iloc[0])}/a.parquet']), \
             patch.object(transform, 'read_parquet_from_s3', return_value=bronze.drop(columns='ano').copy()):
            validator.validate_source_reconciliation(gold)

    def test_dynamic_horizons_nulls_and_parquet(self):
        for entity in ['brasil', 'uf', 'municipio']:
            for horizon in [(2024, 2025, 2026, 2027, 2028, 2029, 2030), (2035, 2037)]:
                with self.subTest(entity=entity, horizon=horizon):
                    transform, validator, bronze = self.fixture(entity, horizon=horizon)
                    silver = transform.transform_wide_to_long(bronze)
                    self.assertEqual(len(silver), len(bronze) * len(horizon))
                    self.assertEqual(set(silver.ano_meta), set(horizon))
                    self.assertEqual(silver.meta_alfabetizacao.isna().sum(), 1)
                    transform.validate_quality(silver)
                    self.validate_silver(validator, silver)
                    self.reconcile(transform, validator, bronze, silver)
                    buffer = BytesIO()
                    silver.to_parquet(buffer, index=False)
                    buffer.seek(0)
                    restored = pd.read_parquet(buffer)
                    self.validate_silver(validator, restored)
                    self.reconcile(transform, validator, bronze, restored)

    def test_reconciliation_detects_loss_values_and_extra_years(self):
        for entity in ['brasil', 'uf', 'municipio']:
            transform, validator, bronze = self.fixture(entity)
            silver = transform.transform_wide_to_long(bronze)
            corrupt = silver.copy()
            corrupt['meta_alfabetizacao'] = [0., 80.]
            extra = silver.iloc[[1]].copy()
            extra['ano_meta'] = 2040
            for bad in [silver.iloc[:1], corrupt, pd.concat([silver, extra], ignore_index=True)]:
                with self.subTest(entity=entity, rows=len(bad)):
                    with self.assertRaises(ValueError):
                        self.reconcile(transform, validator, bronze, bad)

    def test_mixed_source_horizons_processed_per_file(self):
        for entity in ['brasil', 'uf', 'municipio']:
            transform, validator, first = self.fixture(entity, 2032, (2035,))
            _, _, second = self.fixture(entity, 2033, (2036, 2038))
            files = ['bronze/ano=2032/a.parquet', 'bronze/ano=2033/b.parquet']
            frames = {files[0]: first.drop(columns='ano'), files[1]: second.drop(columns='ano')}
            with patch.object(transform, 'list_parquet_files', return_value=files), \
                 patch.object(transform, 'read_parquet_from_s3', side_effect=lambda key: frames[key].copy()), \
                 patch.object(transform, 'write_silver') as write:
                transform.main()
                actual = write.call_args.args[0]
                self.assertEqual(len(actual), 3)
                self.assertEqual(actual.groupby('safra').size().to_dict(), {2032: 1, 2033: 2})
                validator.validate_source_reconciliation(actual)

    def test_invalid_schema_year_key_and_range_rejected(self):
        for entity in ['brasil', 'uf', 'municipio']:
            transform, validator, bronze = self.fixture(entity)
            for bad in [bronze.rename(columns={'meta_alfabetizacao_2035': 'meta_alfabetizacao_foo'}),
                        bronze.drop(columns=transform.get_meta_columns(bronze))]:
                with self.assertRaises(ValueError):
                    transform.validate_bronze_schema(bad)
            silver = transform.transform_wide_to_long(bronze)
            fractional = silver.copy()
            fractional['ano_meta'] = [2035.5, 2037.]
            with self.assertRaises(ValueError):
                validator.validate_ano_meta(fractional)
            null_key = silver.copy()
            null_key['rede'] = None
            with self.assertRaises(ValueError):
                validator.validate_ano_meta(null_key)
            out_of_range = silver.copy()
            out_of_range['meta_alfabetizacao'] = [None, 101.]
            with self.assertRaises(AssertionError):
                validator.validate_ranges(out_of_range)
            with self.assertRaises(AssertionError):
                validator.validate_natural_key(pd.concat([silver, silver]))

    def test_student_metadata_aggregates_all_files(self):
        validator = importlib.import_module('validate_alunos_silver')
        files = ['a/ano=2032/a.parquet', 'a/ano=2032/b.parquet']
        with patch.object(validator, 'list_parquet_files', return_value=files), \
             patch.object(validator, 'get_parquet_metadata', side_effect=[(2, {'id_aluno'}), (3, {'id_aluno'})]):
            self.assertEqual(validator.get_layer_metadata('a')[2032]['rows'], 5)
        with patch.object(validator, 'list_parquet_files', return_value=files), \
             patch.object(validator, 'get_parquet_metadata', side_effect=[(2, {'a'}), (3, {'b'})]):
            with self.assertRaises(ValueError):
                validator.get_layer_metadata('a')
        validator.validate_counts({2032: {'rows': 5}}, {2032: {'rows': 5}})
        for target in [{2032: {'rows': 4}}, {2033: {'rows': 5}}, {2032: {'rows': 5}, 2033: {'rows': 0}}]:
            with self.assertRaises(ValueError):
                validator.validate_counts({2032: {'rows': 5}}, target)

    def test_student_transform_preserves_multiple_files(self):
        transform = importlib.import_module('transform_alunos_silver')
        frame = pd.DataFrame({'id_municipio': ['1234567'], 'id_escola': ['1'], 'id_aluno': ['a'],
            'caderno': ['1'], 'serie': ['2'], 'rede': ['3'], 'presenca': [1],
            'preenchimento_caderno': [1], 'alfabetizado': [1], 'proficiencia': [600.], 'peso_aluno': [1.]})
        with patch.object(transform, 'read_parquet_from_s3', side_effect=[frame, frame.assign(id_aluno='b')]), \
             patch.object(transform, 'write_silver_partition') as write:
            transform.process_partition(['a/ano=2032/a.parquet', 'a/ano=2032/b.parquet'])
            self.assertEqual(write.call_count, 1)
            self.assertEqual(len(write.call_args.kwargs['df']), 2)
        with self.assertRaises(ValueError):
            transform.process_partition(['a/ano=2032/a.parquet', 'a/ano=2033/a.parquet'])

    def test_dictionary_has_no_snapshot_counts(self):
        validator = importlib.import_module('validate_dicionario_silver')
        frame = pd.DataFrame({'coluna': ['rede'], 'valor': ['3']})
        validator.validate_volume(frame, frame.copy())
        validator.validate_schema(frame, frame.copy())
        with self.assertRaises(AssertionError):
            validator.validate_volume(frame, frame.iloc[:0])
        with self.assertRaises(AssertionError):
            validator.validate_schema(frame, frame.assign(extra='x'))

    def run(self, result=None):
        with contextlib.redirect_stdout(StringIO()):
            return super().run(result)


if __name__ == '__main__':
    unittest.main()
