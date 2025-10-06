#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pruebas unitarias para tools/validators.py
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json

# Agregar directorio de tools al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

from validators import (
    ValidationError,
    FatalError,
    validate_env_var,
    validate_db_connections
)


class TestValidators(unittest.TestCase):
    """Pruebas para validadores de entorno"""
    
    def test_validate_env_var_required_present(self):
        """Prueba variable requerida que está presente"""
        with patch.dict(os.environ, {'TEST_VAR': 'test_value'}):
            result = validate_env_var('TEST_VAR', required=True)
            self.assertEqual(result, 'test_value')
    
    def test_validate_env_var_required_missing(self):
        """Prueba variable requerida que falta"""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValidationError):
                validate_env_var('MISSING_VAR', required=True)
    
    def test_validate_env_var_optional_with_default(self):
        """Prueba variable opcional con valor por defecto"""
        with patch.dict(os.environ, {}, clear=True):
            result = validate_env_var('OPTIONAL_VAR', required=False, default='default_value')
            self.assertEqual(result, 'default_value')
    
    def test_validate_db_connections_valid_json(self):
        """Prueba validación de DB_CONNECTIONS válido"""
        valid_json = json.dumps([{
            "name": "test_db",
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "pass": "password",
            "db": "test"
        }])
        
        result = validate_db_connections(valid_json)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'test_db')
    
    def test_validate_db_connections_invalid_json(self):
        """Prueba validación de DB_CONNECTIONS inválido"""
        invalid_json = "{not valid json"
        
        with self.assertRaises(ValidationError):
            validate_db_connections(invalid_json)
    
    def test_validate_db_connections_empty_list(self):
        """Prueba validación de DB_CONNECTIONS vacío"""
        empty_json = "[]"
        result = validate_db_connections(empty_json)
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()
