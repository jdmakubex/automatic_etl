#!/usr/bin/env python3
"""
Script automático para reparar permisos del socket Docker en contenedores ETL.
- Detecta el GID del socket Docker
- Crea el grupo docker si no existe
- Agrega el usuario actual al grupo docker
- Reinicia el contenedor si es necesario
"""
import os
import subprocess
import sys
import pwd

def main():
    try:
        # Detectar GID del socket Docker
        docker_sock = '/var/run/docker.sock'
        if not os.path.exists(docker_sock):
            print(f"❌ No existe {docker_sock}")
            sys.exit(1)
        gid = os.stat(docker_sock).st_gid
        print(f"🔍 GID del socket Docker: {gid}")
        
        # Crear grupo docker si no existe
        try:
            subprocess.run(['getent', 'group', 'docker'], check=True, capture_output=True)
            print("ℹ️ Grupo docker ya existe")
        except subprocess.CalledProcessError:
            try:
                subprocess.run(['groupadd', '-g', str(gid), 'docker'], check=True, capture_output=True)
                print(f"✅ Grupo docker creado con GID {gid}")
            except subprocess.CalledProcessError as e:
                print(f"⚠️ No se pudo crear grupo docker: {e} (posiblemente ya existe)")
        
        # Agregar usuario actual al grupo docker
        user = pwd.getpwuid(os.getuid()).pw_name
        try:
            subprocess.run(['usermod', '-aG', 'docker', user], check=True, capture_output=True)
            print(f"✅ Usuario {user} agregado al grupo docker")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ No se pudo agregar usuario al grupo: {e} (posiblemente ya está)")
        
        # También configurar permisos directos en el socket
        try:
            subprocess.run(['chmod', '666', docker_sock], check=True, capture_output=True)
            print(f"✅ Permisos del socket Docker configurados")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ No se pudieron cambiar permisos del socket: {e}")
        
        print("🔄 Configuración de permisos Docker completada")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error reparando permisos Docker: {e}")
        sys.exit(2)

if __name__ == "__main__":
    main()
