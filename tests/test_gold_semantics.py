"""Regressões semânticas com dados sintéticos; não acessam AWS."""
import importlib
from io import BytesIO
from pathlib import Path
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src' / 'gold'))


class GoldSemanticsTests(unittest.TestCase):
    def fixtures(self, entity):
        identifier = 'id_municipio' if entity == 'municipio' else 'sigla_uf'
        ids = ['1234567', '2345678', '3456789', '4567890'] if entity == 'municipio' else ['SP', 'RJ', 'MG', 'ES']
        results = pd.DataFrame({identifier: ids, 'ano': [2026]*4, 'serie': ['2']*4,
                                'rede': ['3' if entity == 'municipio' else '5']*4,
                                'taxa_alfabetizacao': [70., 40., 50., 60.]})
        targets = pd.DataFrame({identifier: [ids[0], ids[0], ids[1], ids[2], ids[0]],
                                'safra': [2025, 2026, 2026, 2026, 2027], 'ano_meta': [2026]*5,
                                'meta_alfabetizacao': [80., 60., 50., float('nan'), 99.],
                                'percentual_participacao': [90.]*5})
        if entity == 'municipio':
            targets['nivel_alfabetizacao'] = 2
        builder = importlib.import_module(f'build_meta_vs_resultado_{entity}')
        validator = importlib.import_module(f'validate_meta_vs_resultado_{entity}')
        return builder, validator, results, targets

    def validate(self, validator, frame):
        for name in ['validate_schema', 'validate_required_fields', 'validate_year',
                     'validate_natural_key', 'validate_gap', 'validate_atingiu_meta']:
            getattr(validator, name)(frame)

    def test_nullable_comparison_and_parquet(self):
        for entity in ['municipio', 'uf']:
            with self.subTest(entity=entity):
                builder, validator, results, targets = self.fixtures(entity)
                gold = builder.build_gold(results, targets)
                builder.validate_gold(gold, results)
                self.validate(validator, gold)
                self.assertEqual(gold.atingiu_meta.iloc[:2].tolist(), [True, False])
                self.assertTrue(gold.atingiu_meta.iloc[2:].isna().all())
                self.assertEqual(gold.safra_meta.iloc[0], 2026)
                self.assertEqual(len(gold), len(results))
                buffer = BytesIO()
                gold.to_parquet(buffer, index=False)
                buffer.seek(0)
                self.validate(validator, pd.read_parquet(buffer))

    def test_semantic_corruption_rejected(self):
        for entity in ['municipio', 'uf']:
            builder, validator, results, targets = self.fixtures(entity)
            gold = builder.build_gold(results, targets)
            for column, row, value in [('atingiu_meta', 2, False), ('atingiu_meta', 0, pd.NA),
                                       ('gap_meta', 2, 0.), ('gap_meta', 0, float('nan')),
                                       ('meta_disponivel', 2, True)]:
                with self.subTest(entity=entity, column=column, row=row):
                    bad = gold.copy()
                    bad.loc[row, column] = value
                    with self.assertRaises(ValueError):
                        self.validate(validator, bad)

    def test_versions_duplicates_and_empty_batches(self):
        for entity in ['municipio', 'uf']:
            builder, validator, results, targets = self.fixtures(entity)
            with self.assertRaises(ValueError):
                builder.build_gold(results, pd.concat([targets, targets.iloc[[1]]]))
            empty = builder.build_gold(results.iloc[:0], targets)
            builder.validate_gold(empty, results.iloc[:0])
            self.validate(validator, empty)

    def test_indicator_preserves_base_and_reads_all_files(self):
        builder, _, results, targets = self.fixtures('municipio')
        base = builder.build_gold(results, targets)
        indicator = importlib.import_module('build_indicador_municipio')
        students = pd.DataFrame({'id_aluno': ['a', 'b', 'c'],
            'id_municipio': ['1234567', '1234567', '3456789'], 'rede': ['3']*3,
            'presenca': [1, 1, 0], 'preenchimento_caderno': [1, 1, 0],
            'alfabetizado': [1, 0, None], 'proficiencia': [600., 400., None],
            'peso_aluno': [1., 1., 0.], 'aluno_valido_avaliacao': [True, True, False]})
        gold = indicator.build_gold(base, indicator.aggregate_alunos(indicator.prepare_alunos(students)))
        indicator.validate_gold(gold, base)
        self.assertEqual(gold.loc[0, 'taxa_alfabetizacao_amostra'], 50.)
        self.assertEqual(gold.loc[0, 'taxa_alfabetizacao_ponderada'], 50.)
        self.assertTrue(gold.atingiu_meta.iloc[2:].isna().all())
        from unittest.mock import patch
        with patch.object(indicator, 'list_parquet_files', return_value=[
            'silver/alunos/ano=2026/a.parquet', 'silver/alunos/ano=2026/b.parquet',
            'silver/alunos/ano=2027/c.parquet']):
            self.assertEqual(len(indicator.find_alunos_partitions(2026)), 2)


if __name__ == '__main__':
    unittest.main()
