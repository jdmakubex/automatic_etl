#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pruebas unitarias para tools/verify_dependencies.py
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json

# Agregar directorio de tools al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

from verify_dependencies import save_verification_results, DependencyError


class TestDependencyVerification(unittest.TestCase):
    """Pruebas para verificación de dependencias"""
    
    def test_save_verification_results_creates_file(self):
        """Prueba que save_verification_results crea archivo correctamente"""
        results = {
            "timestamp": "2025-01-01T00:00:00Z",
            "checks": {"clickhouse_ready": True},
            "sequence_valid": True,
            "duration_seconds": 1.5
        }
        
        output_file = "/tmp/test_dependency_output.json"
        
        # Limpiar archivo si existe
        if os.path.exists(output_file):
            os.remove(output_file)
        
        save_verification_results(results, output_file)
        
        # Verificar que el archivo existe
        self.assertTrue(os.path.exists(output_file))
        
        # Verificar contenido
        with open(output_file, 'r') as f:
            saved_data = json.load(f)
        
        self.assertTrue(saved_data['sequence_valid'])
        self.assertEqual(saved_data['duration_seconds'], 1.5)
        
        # Limpiar
        os.remove(output_file)


if __name__ == '__main__':
    unittest.main()
