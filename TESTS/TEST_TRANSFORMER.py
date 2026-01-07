"""Tests unitaires pour les modules de transformation"""
import unittest
import pandas as pd
import sys
sys.path.insert(0, '../SRC')

from TRANSFORM.transform_airquality import transform_airquality_data

class TestTransformer(unittest.TestCase):
    
    def test_transform_aggregates_monthly(self):
        """Vérifie que la transformation agrège par mois"""
        # Données de test
        data = pd.DataFrame({
            'time': pd.date_range('2024-01-01', periods=100, freq='H'),
            'country': ['France'] * 100,
            'year': [2024] * 100,
            'pm2_5': [10.5] * 100,
            'pm10': [20.3] * 100,
            'nitrogen_dioxide': [15.2] * 100,
            'ozone': [30.1] * 100
        })
        result = transform_airquality_data(data)
        self.assertTrue('month' in result.columns)
        self.assertEqual(len(result), 1)  # 1 mois

if __name__ == '__main__':
    unittest.main()
