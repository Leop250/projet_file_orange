"""Tests unitaires pour les modules d'extraction"""
import unittest
import sys
sys.path.insert(0, '../SRC')

from EXTRACT.extract_airquality import extract_air_quality_openmeteo

class TestExtractor(unittest.TestCase):
    
    def test_extract_returns_dataframe(self):
        """Vérifie que l'extraction retourne bien un DataFrame"""
        result = extract_air_quality_openmeteo()
        self.assertIsNotNone(result)
        self.assertTrue(len(result) > 0)

if __name__ == '__main__':
    unittest.main()
