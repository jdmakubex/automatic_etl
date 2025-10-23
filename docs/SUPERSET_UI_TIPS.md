# Superset: tips rápidos de uso (explore y datasets)

- Crear gráficos (charts)
  - Usa el botón "+ Chart" en la barra superior o desde Data > Datasets selecciona un dataset y haz clic en "Explore".
  - En el selector de tipo de gráfico, la pestaña inicial muestra "Recommended"; cambia a "All charts" para ver todos los tipos disponibles.
  - Si ves el selector de tipo de visualización deshabilitado, es probable que estés viendo un chart guardado en modo solo lectura. Haz clic en "Edit chart" (icono de lápiz) o crea un chart nuevo desde "+ Chart".

- “This table already has a dataset”
  - Es un mensaje normal. Superset maneja una sola definición de dataset por tabla. Para crear gráficos, usa ese dataset existente (no es necesario crear otro).

- Fechas y Time Grain
  - Todos los datasets ya marcan automáticamente las columnas de tipo fecha/tiempo y traen el "Time Grain" en None por defecto. Puedes cambiarlo en Explore si lo necesitas.

- Propiedad y permisos
  - El usuario `admin` es propietario de los datasets y charts creados automáticamente para facilitar la edición. Si agregas nuevos usuarios, asígnales acceso vía roles o hazlos owners de los artefactos que necesiten editar.
