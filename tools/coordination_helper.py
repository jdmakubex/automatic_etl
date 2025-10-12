#!/usr/bin/env python3
"""
coordination_helper.py
Sistema de coordinación entre contenedores usando archivos de estado compartidos.
Ejecución recomendada: Docker (todos los contenedores).
Permite comunicación y sincronización entre servicios ETL.
"""

import os
import json
import time
import fcntl
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum

class ServiceStatus(Enum):
    INITIALIZING = "initializing"
    READY = "ready" 
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    MAINTENANCE = "maintenance"

@dataclass
class ServiceState:
    service_name: str
    container_name: str
    status: ServiceStatus
    last_update: datetime
    progress_percent: float = 0.0
    message: str = ""
    data: Dict[str, Any] = None
    dependencies_ready: List[str] = None
    
    def __post_init__(self):
        if self.data is None:
            self.data = {}
        if self.dependencies_ready is None:
            self.dependencies_ready = []

class CoordinationHelper:
    """Helper para coordinar estado entre contenedores ETL"""
    
    def __init__(self, service_name: str, container_name: str = None):
        self.service_name = service_name
        self.container_name = container_name or os.environ.get('HOSTNAME', service_name)
        self.state_dir = Path('/app/logs/coordination')
        self.state_file = self.state_dir / f"{service_name}_state.json"
        self.global_state_file = self.state_dir / "global_coordination.json"
        
        # Crear directorio de estado si no existe
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Logger
        self.logger = logging.getLogger(f'Coordination.{service_name}')
        
        # Estado actual
        self.current_state = ServiceState(
            service_name=service_name,
            container_name=self.container_name,
            status=ServiceStatus.INITIALIZING,
            last_update=datetime.now()
        )
        
        # Registrar servicio inicialmente
        self.update_status(ServiceStatus.INITIALIZING, "Servicio iniciando...")
    
    def _lock_file_operation(self, file_path: Path, operation_func, mode='r+'):
        """Operación thread-safe con lock de archivo"""
        try:
            # Crear archivo si no existe
            if not file_path.exists():
                file_path.touch()
                if mode in ['r+', 'a+']:
                    file_path.write_text('{}')
            
            with open(file_path, mode) as f:
                # Obtener lock exclusivo
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    return operation_func(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            self.logger.error(f"Error en operación de archivo {file_path}: {e}")
            return None
    
    def update_status(self, status: ServiceStatus, message: str = "", progress: float = 0.0, data: Dict[str, Any] = None):
        """Actualizar estado del servicio"""
        self.current_state.status = status
        self.current_state.message = message
        self.current_state.progress_percent = progress
        self.current_state.last_update = datetime.now()
        
        if data:
            self.current_state.data.update(data)
        
        # Escribir estado individual del servicio
        def write_state(f):
            state_dict = asdict(self.current_state)
            state_dict['last_update'] = self.current_state.last_update.isoformat()
            state_dict['status'] = status.value
            
            f.seek(0)
            f.truncate()
            json.dump(state_dict, f, indent=2, ensure_ascii=False)
            f.flush()
            return True
        
        self._lock_file_operation(self.state_file, write_state, 'w')
        
        # Actualizar estado global
        self._update_global_state()
        
        self.logger.info(f"Estado actualizado: {status.value} - {message}")
    
    def _update_global_state(self):
        """Actualizar archivo de estado global con todos los servicios"""
        def update_global(f):
            try:
                f.seek(0)
                content = f.read()
                global_state = json.loads(content) if content.strip() else {}
            except (json.JSONDecodeError, ValueError):
                global_state = {}
            
            # Actualizar entrada del servicio actual
            service_data = asdict(self.current_state)
            service_data['last_update'] = self.current_state.last_update.isoformat()
            service_data['status'] = self.current_state.status.value
            
            global_state[self.service_name] = service_data
            global_state['last_global_update'] = datetime.now().isoformat()
            
            f.seek(0)
            f.truncate()
            json.dump(global_state, f, indent=2, ensure_ascii=False)
            f.flush()
            return True
        
        self._lock_file_operation(self.global_state_file, update_global, 'w')
    
    def get_service_state(self, service_name: str) -> Optional[ServiceState]:
        """Obtener estado de otro servicio"""
        service_file = self.state_dir / f"{service_name}_state.json"
        
        def read_state(f):
            try:
                content = f.read()
                if not content.strip():
                    return None
                
                data = json.loads(content)
                # Convertir status string a enum
                data['status'] = ServiceStatus(data['status'])
                data['last_update'] = datetime.fromisoformat(data['last_update'])
                
                return ServiceState(**data)
            except Exception as e:
                self.logger.warning(f"Error leyendo estado de {service_name}: {e}")
                return None
        
        return self._lock_file_operation(service_file, read_state, 'r')
    
    def get_all_services_state(self) -> Dict[str, ServiceState]:
        """Obtener estado de todos los servicios registrados"""
        def read_global(f):
            try:
                content = f.read()
                if not content.strip():
                    return {}
                
                data = json.loads(content)
                services = {}
                
                for service_name, service_data in data.items():
                    if service_name == 'last_global_update':
                        continue
                    
                    try:
                        service_data['status'] = ServiceStatus(service_data['status'])
                        service_data['last_update'] = datetime.fromisoformat(service_data['last_update'])
                        services[service_name] = ServiceState(**service_data)
                    except Exception as e:
                        self.logger.warning(f"Error procesando estado de {service_name}: {e}")
                
                return services
            except Exception as e:
                self.logger.warning(f"Error leyendo estado global: {e}")
                return {}
        
        return self._lock_file_operation(self.global_state_file, read_global, 'r') or {}
    
    def wait_for_dependencies(self, dependencies: List[str], timeout_seconds: int = 300, required_status: ServiceStatus = ServiceStatus.READY) -> bool:
        """Esperar que dependencias alcancen un estado específico"""
        self.logger.info(f"Esperando dependencias: {dependencies}")
        self.update_status(ServiceStatus.INITIALIZING, f"Esperando dependencias: {', '.join(dependencies)}")
        
        start_time = time.time()
        
        while (time.time() - start_time) < timeout_seconds:
            all_ready = True
            status_report = []
            
            for dep_service in dependencies:
                dep_state = self.get_service_state(dep_service)
                
                if dep_state is None:
                    all_ready = False
                    status_report.append(f"{dep_service}: no registrado")
                    continue
                
                # Verificar si está en estado requerido o superior
                if dep_state.status.value in [ServiceStatus.READY.value, ServiceStatus.COMPLETED.value]:
                    status_report.append(f"{dep_service}: ✅")
                elif dep_state.status == ServiceStatus.FAILED:
                    self.logger.error(f"Dependencia {dep_service} falló: {dep_state.message}")
                    return False
                else:
                    all_ready = False
                    status_report.append(f"{dep_service}: {dep_state.status.value}")
            
            if all_ready:
                self.logger.info(f"✅ Todas las dependencias están listas: {', '.join(status_report)}")
                self.current_state.dependencies_ready = dependencies.copy()
                return True
            
            # Log de progreso cada 30 segundos
            elapsed = time.time() - start_time
            if int(elapsed) % 30 == 0:
                self.logger.info(f"Esperando dependencias ({elapsed:.0f}s): {', '.join(status_report)}")
            
            time.sleep(5)
        
        self.logger.error(f"❌ Timeout esperando dependencias después de {timeout_seconds}s")
        return False
    
    def signal_ready(self, message: str = "Servicio listo"):
        """Marcar servicio como listo"""
        self.update_status(ServiceStatus.READY, message)
    
    def signal_processing(self, message: str = "Procesando...", progress: float = 0.0):
        """Marcar servicio como procesando"""
        self.update_status(ServiceStatus.PROCESSING, message, progress)
    
    def signal_completed(self, message: str = "Completado exitosamente", data: Dict[str, Any] = None):
        """Marcar servicio como completado"""
        self.update_status(ServiceStatus.COMPLETED, message, 100.0, data)
    
    def signal_failed(self, error_message: str, data: Dict[str, Any] = None):
        """Marcar servicio como fallido"""
        self.update_status(ServiceStatus.FAILED, error_message, data=data)
    
    def wait_for_phase_completion(self, phase_services: List[str], timeout_seconds: int = 600) -> bool:
        """Esperar que una fase completa (conjunto de servicios) termine"""
        self.logger.info(f"Esperando completación de fase: {phase_services}")
        
        start_time = time.time()
        
        while (time.time() - start_time) < timeout_seconds:
            all_services_state = self.get_all_services_state()
            completed_count = 0
            failed_count = 0
            status_report = []
            
            for service in phase_services:
                if service in all_services_state:
                    state = all_services_state[service]
                    if state.status == ServiceStatus.COMPLETED:
                        completed_count += 1
                        status_report.append(f"{service}: ✅")
                    elif state.status == ServiceStatus.FAILED:
                        failed_count += 1
                        status_report.append(f"{service}: ❌")
                    else:
                        status_report.append(f"{service}: {state.status.value}")
                else:
                    status_report.append(f"{service}: no registrado")
            
            # Verificar condiciones de finalización
            if completed_count == len(phase_services):
                self.logger.info(f"✅ Fase completada: todos los servicios terminaron")
                return True
            elif failed_count > 0:
                self.logger.error(f"❌ Fase falló: {failed_count} servicios fallaron")
                return False
            
            # Log de progreso
            elapsed = time.time() - start_time
            if int(elapsed) % 30 == 0:
                progress = (completed_count / len(phase_services)) * 100
                self.logger.info(f"Progreso de fase ({elapsed:.0f}s): {progress:.1f}% - {', '.join(status_report)}")
            
            time.sleep(10)
        
        self.logger.error(f"❌ Timeout esperando fase después de {timeout_seconds}s")
        return False
    
    def get_coordination_summary(self) -> Dict[str, Any]:
        """Obtener resumen del estado de coordinación"""
        all_states = self.get_all_services_state()
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_services": len(all_states),
            "status_counts": {},
            "services": {}
        }
        
        for service_name, state in all_states.items():
            status_key = state.status.value
            summary["status_counts"][status_key] = summary["status_counts"].get(status_key, 0) + 1
            
            summary["services"][service_name] = {
                "status": status_key,
                "message": state.message,
                "progress": state.progress_percent,
                "last_update": state.last_update.isoformat(),
                "container": state.container_name
            }
        
        return summary
    
    def cleanup_coordination_files(self):
        """Limpiar archivos de coordinación antiguos"""
        try:
            for file_path in self.state_dir.glob("*_state.json"):
                if file_path.exists():
                    file_path.unlink()
            
            if self.global_state_file.exists():
                self.global_state_file.unlink()
            
            self.logger.info("Archivos de coordinación limpiados")
        except Exception as e:
            self.logger.warning(f"Error limpiando archivos de coordinación: {e}")

# Funciones de utilidad para usar en scripts
def init_service_coordination(service_name: str) -> CoordinationHelper:
    """Inicializar coordinación para un servicio"""
    return CoordinationHelper(service_name)

def wait_for_service(service_name: str, target_service: str, timeout: int = 300) -> bool:
    """Helper rápido para esperar otro servicio"""
    coord = CoordinationHelper(service_name)
    return coord.wait_for_dependencies([target_service], timeout)

def signal_service_ready(service_name: str, message: str = "Listo"):
    """Helper rápido para marcar servicio como listo"""
    coord = CoordinationHelper(service_name)
    coord.signal_ready(message)

def signal_service_completed(service_name: str, message: str = "Completado", data: Dict[str, Any] = None):
    """Helper rápido para marcar servicio como completado"""
    coord = CoordinationHelper(service_name)
    coord.signal_completed(message, data)

if __name__ == "__main__":
    # Demo/Test de coordinación
    import sys
    
    if len(sys.argv) < 2:
        print("Uso: python coordination_helper.py <service_name> [action]")
        print("Acciones: ready, processing, completed, failed, status")
        sys.exit(1)
    
    service_name = sys.argv[1]
    action = sys.argv[2] if len(sys.argv) > 2 else "status"
    
    coord = CoordinationHelper(service_name)
    
    if action == "ready":
        coord.signal_ready("Servicio marcado como listo manualmente")
    elif action == "processing":
        coord.signal_processing("Procesando manualmente")
    elif action == "completed":
        coord.signal_completed("Completado manualmente")
    elif action == "failed":
        coord.signal_failed("Marcado como fallido manualmente")
    elif action == "status":
        summary = coord.get_coordination_summary()
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(f"Acción desconocida: {action}")
        sys.exit(1)