#!/usr/bin/env python3
"""
Pruebas unitarias para el Agente de Ciclo Automático ETL
"""

import unittest
import os
import sys
import tempfile
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Agregar path para importar el módulo
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

try:
    from auto_etl_cycle_agent import (
        AutoETLCycleAgent,
        CycleStatus,
        CycleResult
    )
except ImportError as e:
    print(f"Error importando módulo: {e}")
    print("Asegúrate de que auto_etl_cycle_agent.py existe en tools/")
    sys.exit(1)


class TestAutoETLCycleAgent(unittest.TestCase):
    """Pruebas para AutoETLCycleAgent"""
    
    def setUp(self):
        """Configurar pruebas"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Crear estructura de directorios necesaria
        os.makedirs(os.path.join(self.temp_dir, 'logs'), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, 'tools'), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, 'generated', 'default'), exist_ok=True)
    
    def tearDown(self):
        """Limpiar después de pruebas"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_agent_initialization(self):
        """Verificar inicialización del agente"""
        agent = AutoETLCycleAgent(max_iterations=3, debug=False)
        
        self.assertEqual(agent.max_iterations, 3)
        self.assertEqual(agent.current_iteration, 0)
        self.assertFalse(agent.overall_success)
        self.assertEqual(len(agent.cycle_results), 0)
        self.assertIsNotNone(agent.execution_id)
        self.assertIsNotNone(agent.logger)
    
    def test_cycle_result_creation(self):
        """Verificar creación de CycleResult"""
        start_time = datetime.now()
        cycle = CycleResult(
            iteration=1,
            status=CycleStatus.INITIALIZING,
            start_time=start_time
        )
        
        self.assertEqual(cycle.iteration, 1)
        self.assertEqual(cycle.status, CycleStatus.INITIALIZING)
        self.assertEqual(cycle.start_time, start_time)
        self.assertIsNone(cycle.end_time)
        self.assertEqual(cycle.duration_seconds, 0.0)
        self.assertFalse(cycle.success)
        self.assertEqual(len(cycle.errors_detected), 0)
        self.assertEqual(len(cycle.adjustments_made), 0)
    
    def test_extract_errors_from_output(self):
        """Verificar extracción de errores del output"""
        agent = AutoETLCycleAgent(max_iterations=1, debug=False)
        
        stdout = """
        ✅ Paso 1 completado
        ❌ ERROR: Conexión falló
        ✅ Paso 2 completado
        ERROR: No se pudo crear tabla
        """
        stderr = "CRITICAL: Servicio no disponible"
        
        errors = agent._extract_errors_from_output(stdout, stderr)
        
        self.assertIsInstance(errors, list)
        self.assertGreater(len(errors), 0)
        # Verificar que se detectaron errores
        error_texts = ' '.join(errors)
        self.assertTrue('ERROR' in error_texts or 'CRITICAL' in error_texts)
    
    def test_analyze_and_adjust_error(self):
        """Verificar análisis y ajuste de errores"""
        agent = AutoETLCycleAgent(max_iterations=1, debug=False)
        
        # Test diferentes tipos de errores
        test_cases = [
            ("Connection refused to database", "Esperar más tiempo"),
            ("Timeout waiting for service", "Incrementar timeout"),
            ("Permission denied for user", "Verificar permisos"),
            ("File not found: config.json", "Verificar paths"),
            ("Table 'test' already exists", None),  # Sin ajuste específico
        ]
        
        for error, expected_keyword in test_cases:
            adjustment = agent._analyze_and_adjust_error(error)
            if expected_keyword:
                self.assertIsInstance(adjustment, str)
                # Verificar que el ajuste contiene alguna keyword relevante
            else:
                # Puede ser None si no hay ajuste específico
                pass
    
    @patch('subprocess.run')
    def test_step_1_commit_no_changes(self, mock_run):
        """Verificar paso 1 cuando no hay cambios"""
        # Mock git status sin cambios
        mock_run.return_value = Mock(
            returncode=0,
            stdout='',
            stderr=''
        )
        
        with patch('os.chdir'):
            agent = AutoETLCycleAgent(max_iterations=1, debug=False)
            success, adjustments = agent.step_1_commit_and_push(1)
        
        self.assertTrue(success)
        self.assertEqual(len(adjustments), 0)
    
    @patch('subprocess.run')
    def test_step_2_cleanup_success(self, mock_run):
        """Verificar paso 2 de limpieza exitosa"""
        # Mock ejecución exitosa del script de limpieza
        mock_run.return_value = Mock(
            returncode=0,
            stdout='Limpieza completada',
            stderr=''
        )
        
        # Crear script de limpieza mock
        cleanup_script = os.path.join(self.temp_dir, 'tools', 'cleanup_all.py')
        with open(cleanup_script, 'w') as f:
            f.write('# Mock cleanup script')
        
        with patch('os.chdir'):
            agent = AutoETLCycleAgent(max_iterations=1, debug=False)
            agent.work_dir = self.temp_dir
            success, adjustments = agent.step_2_complete_cleanup(1)
        
        self.assertTrue(success)
        self.assertGreater(len(adjustments), 0)
    
    @patch('subprocess.run')
    def test_step_3_orchestrator_success(self, mock_run):
        """Verificar paso 3 con orquestador exitoso"""
        # Mock ejecución exitosa del orquestador
        mock_run.return_value = Mock(
            returncode=0,
            stdout='✅ Pipeline ETL completado exitosamente',
            stderr=''
        )
        
        # Crear script orquestador mock
        orchestrator_script = os.path.join(self.temp_dir, 'tools', 'orchestrate_etl_init.py')
        with open(orchestrator_script, 'w') as f:
            f.write('# Mock orchestrator')
        
        with patch('os.chdir'):
            agent = AutoETLCycleAgent(max_iterations=1, debug=False)
            agent.work_dir = self.temp_dir
            success, adjustments, details = agent.step_3_execute_orchestrator(1)
        
        self.assertTrue(success)
        self.assertGreater(len(adjustments), 0)
        self.assertIn('orchestrator_returncode', details)
        self.assertEqual(details['orchestrator_returncode'], 0)
    
    @patch('subprocess.run')
    def test_step_3_orchestrator_failure(self, mock_run):
        """Verificar paso 3 con orquestador fallido"""
        # Mock ejecución fallida del orquestador
        mock_run.return_value = Mock(
            returncode=1,
            stdout='',
            stderr='❌ ERROR: Conexión a base de datos falló'
        )
        
        # Crear script orquestador mock
        orchestrator_script = os.path.join(self.temp_dir, 'tools', 'orchestrate_etl_init.py')
        with open(orchestrator_script, 'w') as f:
            f.write('# Mock orchestrator')
        
        with patch('os.chdir'):
            agent = AutoETLCycleAgent(max_iterations=1, debug=False)
            agent.work_dir = self.temp_dir
            success, adjustments, details = agent.step_3_execute_orchestrator(1)
        
        self.assertFalse(success)
        self.assertIn('orchestrator_returncode', details)
        self.assertEqual(details['orchestrator_returncode'], 1)
    
    def test_step_4_validate_success(self):
        """Verificar paso 4 con validación exitosa"""
        agent = AutoETLCycleAgent(max_iterations=1, debug=False)
        agent.work_dir = self.temp_dir
        
        # Crear logs simulados
        logs_dir = os.path.join(self.temp_dir, 'logs')
        with open(os.path.join(logs_dir, 'test.log'), 'w') as f:
            f.write('Test log')
        
        orchestrator_details = {
            'orchestrator_stdout': '✅ EXITOSA Pipeline ETL operativo',
            'orchestrator_stderr': '',
            'orchestrator_returncode': 0
        }
        
        with patch.object(agent, '_run_additional_validations') as mock_validate:
            mock_validate.return_value = {
                'passed': True,
                'errors': [],
                'checks': {'log_files_created': True}
            }
            
            success, errors = agent.step_4_validate_results(1, orchestrator_details)
        
        self.assertTrue(success)
        self.assertEqual(len(errors), 0)
    
    def test_step_4_validate_failure(self):
        """Verificar paso 4 con validación fallida"""
        agent = AutoETLCycleAgent(max_iterations=1, debug=False)
        agent.work_dir = self.temp_dir
        
        orchestrator_details = {
            'orchestrator_stdout': '❌ FALLÓ ❌ ERROR ❌ FAILED',
            'orchestrator_stderr': 'Multiple errors occurred',
            'orchestrator_returncode': 1
        }
        
        with patch.object(agent, '_run_additional_validations') as mock_validate:
            mock_validate.return_value = {
                'passed': False,
                'errors': ['Validation error 1', 'Validation error 2'],
                'checks': {'log_files_created': False}
            }
            
            success, errors = agent.step_4_validate_results(1, orchestrator_details)
        
        self.assertFalse(success)
        self.assertGreater(len(errors), 0)
    
    def test_step_5_automatic_adjustments(self):
        """Verificar paso 5 con ajustes automáticos"""
        agent = AutoETLCycleAgent(max_iterations=1, debug=False)
        
        detected_errors = [
            "Connection refused to database",
            "Timeout waiting for service",
            "Permission denied"
        ]
        
        success, adjustments = agent.step_5_automatic_adjustments(1, detected_errors)
        
        self.assertTrue(success)  # Siempre retorna True (no crítico)
        # Puede tener ajustes o no, dependiendo del análisis
    
    def test_save_execution_report(self):
        """Verificar guardado de reporte de ejecución"""
        agent = AutoETLCycleAgent(max_iterations=1, debug=False)
        agent.work_dir = self.temp_dir
        
        # Agregar un ciclo de prueba
        cycle = CycleResult(
            iteration=1,
            status=CycleStatus.SUCCESS,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=10.5,
            success=True
        )
        agent.cycle_results.append(cycle)
        agent.overall_success = True
        
        # Guardar reporte
        agent._save_execution_report(10.5)
        
        # Verificar que se creó el archivo
        logs_dir = os.path.join(self.temp_dir, 'logs')
        report_files = [f for f in os.listdir(logs_dir) if f.startswith('auto_cycle_report_')]
        
        self.assertGreater(len(report_files), 0)
        
        # Verificar contenido del JSON
        report_file = os.path.join(logs_dir, report_files[0])
        with open(report_file, 'r') as f:
            report_data = json.load(f)
        
        self.assertIn('execution_id', report_data)
        self.assertIn('overall_success', report_data)
        self.assertTrue(report_data['overall_success'])
        self.assertIn('cycles', report_data)
        self.assertEqual(len(report_data['cycles']), 1)


class TestCycleStatus(unittest.TestCase):
    """Pruebas para el enum CycleStatus"""
    
    def test_cycle_status_values(self):
        """Verificar valores del enum"""
        self.assertEqual(CycleStatus.INITIALIZING.value, "initializing")
        self.assertEqual(CycleStatus.COMMITTING.value, "committing")
        self.assertEqual(CycleStatus.CLEANING.value, "cleaning")
        self.assertEqual(CycleStatus.EXECUTING.value, "executing")
        self.assertEqual(CycleStatus.VALIDATING.value, "validating")
        self.assertEqual(CycleStatus.ADJUSTING.value, "adjusting")
        self.assertEqual(CycleStatus.SUCCESS.value, "success")
        self.assertEqual(CycleStatus.FAILED.value, "failed")


if __name__ == '__main__':
    # Configurar para correr pruebas
    unittest.main(verbosity=2)
