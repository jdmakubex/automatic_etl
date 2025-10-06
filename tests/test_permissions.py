#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pruebas unitarias para tools/test_permissions.py
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json

# Agregar directorio de tools al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

from test_permissions import save_test_results


class TestPermissions(unittest.TestCase):
    """Pruebas para verificación de permisos"""
    
    def test_save_test_results_creates_file(self):
        """Prueba que save_test_results crea archivo correctamente"""
        results = {
            "timestamp": "2025-01-01T00:00:00Z",
            "tests": {"test1": {"passed": True}},
            "summary": {"total": 1, "passed": 1, "failed": 0}
        }
        
        output_file = "/tmp/test_permissions_output.json"
        
        # Limpiar archivo si existe
        if os.path.exists(output_file):
            os.remove(output_file)
        
        save_test_results(results, output_file)
        
        # Verificar que el archivo existe
        self.assertTrue(os.path.exists(output_file))
        
        # Verificar contenido
        with open(output_file, 'r') as f:
            saved_data = json.load(f)
        
        self.assertEqual(saved_data['summary']['total'], 1)
        self.assertEqual(saved_data['summary']['passed'], 1)
        
        # Limpiar
        os.remove(output_file)


if __name__ == '__main__':
    unittest.main()
