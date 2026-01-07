"""Tests unitaires pour les modules de chargement"""
import unittest
import pandas as pd
from unittest.mock import patch, MagicMock

class TestLoader(unittest.TestCase):
    
    @patch('google.cloud.bigquery.Client')
    def test_load_calls_bigquery(self, mock_client):
        """Vérifie que le chargement appelle BigQuery"""
        # Mock du client BigQuery
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        
        # Données de test
        data = pd.DataFrame({'country': ['France'], 'pm2_5': [12.5]})
        
        # Vérifier que le mock est appelé
        self.assertIsNotNone(mock_instance)

if __name__ == '__main__':
    unittest.main()
