import logging
import pandas as pd
import sqlite3
import time
from pathlib import Path

# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('etl_pipeline')


# ============================================================================
# CLASE PRINCIPAL DEL PIPELINE ETL
# ============================================================================

class RobustETLPipeline:
    """Pipeline ETL robusto con manejo de errores, reintentos y transacciones"""
    
    def __init__(self, db_path='etl_database.db'):
        self.db_path = db_path
        self.logger = logging.getLogger('etl_pipeline')
        self.metrics = {'processed': 0, 'errors': 0, 'start_time': None}
    
    def run_pipeline(self):
        """Ejecuta el pipeline completo: Extracción -> Transformación -> Carga"""
        self.metrics['start_time'] = pd.Timestamp.now()
        self.logger.info("=== INICIANDO PIPELINE ETL ROBUSTO ===")
        
        try:
            # Fase 1: Extracción con reintentos
            data = self.extract_with_retry()
            
            # Fase 2: Transformación con validaciones
            transformed_data = self.transform_with_validation(data)
            
            # Fase 3: Carga con transacciones
            self.load_with_transaction(transformed_data)
            
            # Reporte de éxito
            self.report_success()
            
        except Exception as e:
            self.report_failure(e)
            raise
    
    # ========================================================================
    # FASE 1: EXTRACCIÓN CON REINTENTOS
    # ========================================================================
    
    def extract_with_retry(self):
        """Extracción de datos con estrategia de reintentos automáticos"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Intento de extracción #{attempt + 1}")
                
                # Simular extracción de datos (reemplazar con fuente real)
                data = pd.DataFrame({
                    'id': range(1, 101),
                    'valor': [x * 1.1 for x in range(1, 101)],
                    'categoria': ['A', 'B', 'C'] * 33 + ['A']
                })
                
                self.logger.info(f"✓ Extracción exitosa: {len(data)} registros")
                self.metrics['processed'] = len(data)
                return data
                
            except Exception as e:
                self.metrics['errors'] += 1
                self.logger.warning(f"✗ Intento #{attempt + 1} falló: {e}")
                
                if attempt == max_retries - 1:
                    self.logger.error("Máximo de reintentos alcanzado")
                    raise e
                
                time.sleep(1)  # Esperar antes de reintentar
    
    # ========================================================================
    # FASE 2: TRANSFORMACIÓN CON VALIDACIONES
    # ========================================================================
    
    def transform_with_validation(self, data):
        """Transformación de datos con validaciones y logging detallado"""
        self.logger.info("Iniciando transformación de datos")
        original_count = len(data)
        
        try:
            # Validación 1: Verificar datos nulos
            if data.isnull().any().any():
                null_counts = data.isnull().sum()
                self.logger.warning(
                    f"⚠ Valores nulos encontrados: "
                    f"{null_counts[null_counts > 0].to_dict()}"
                )
            
            # Transformación 1: Limpiar datos nulos
            data_clean = data.dropna()
            
            # Transformación 2: Crear nuevas columnas calculadas
            data_clean = data_clean.copy()  # Evitar SettingWithCopyWarning
            data_clean['valor_cuadrado'] = data_clean['valor'] ** 2
            data_clean['categoria_normalizada'] = data_clean['categoria'].str.upper()
            
            # Validación 2: Verificar resultados razonables
            if (data_clean['valor_cuadrado'] < 0).any():
                raise ValueError("Valores cuadrados negativos detectados")
            
            records_lost = original_count - len(data_clean)
            if records_lost > 0:
                self.logger.warning(f"⚠ {records_lost} registros eliminados por limpieza")
            
            self.logger.info(
                f"✓ Transformación exitosa: "
                f"{original_count} -> {len(data_clean)} registros"
            )
            
            return data_clean
            
        except Exception as e:
            self.logger.error(f"✗ Error en transformación: {e}")
            raise
    
    # ========================================================================
    # FASE 3: CARGA CON TRANSACCIONES
    # ========================================================================
    
    def load_with_transaction(self, data):
        """Carga de datos con soporte transaccional y rollback automático"""
        self.logger.info("Iniciando carga a base de datos")
        
        with sqlite3.connect(self.db_path) as conn:
            try:
                # Iniciar transacción explícita
                conn.execute('BEGIN TRANSACTION')
                
                # Crear tabla si no existe
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS datos_transformados (
                        id INTEGER PRIMARY KEY,
                        valor REAL,
                        categoria TEXT,
                        valor_cuadrado REAL,
                        categoria_normalizada TEXT
                    )
                ''')
                
                # Limpiar datos previos (estrategia replace)
                conn.execute('DELETE FROM datos_transformados')
                self.logger.info("Tabla limpiada para nueva carga")
                
                # Insertar nuevos datos
                data.to_sql(
                    'datos_transformados', 
                    conn, 
                    index=False, 
                    if_exists='append'
                )
                
                # Commit de la transacción
                conn.commit()
                
                self.logger.info(f"✓ Carga exitosa: {len(data)} registros insertados")
                
            except Exception as e:
                # Rollback automático por context manager
                conn.rollback()
                self.logger.error(f"✗ Error en carga, rollback ejecutado: {e}")
                raise
    
    # ========================================================================
    # REPORTES Y MÉTRICAS
    # ========================================================================
    
    def report_success(self):
        """Genera reporte de métricas al completar exitosamente"""
        duration = pd.Timestamp.now() - self.metrics['start_time']
        
        self.logger.info("=" * 60)
        self.logger.info("✓ PIPELINE ETL COMPLETADO EXITOSAMENTE")
        self.logger.info("=" * 60)
        self.logger.info(f"Duración total: {duration}")
        self.logger.info(f"Registros procesados: {self.metrics['processed']}")
        self.logger.info(f"Errores encontrados: {self.metrics['errors']}")
        self.logger.info("=" * 60)
    
    def report_failure(self, error):
        """Genera reporte de fallo con detalles del error"""
        duration = pd.Timestamp.now() - self.metrics['start_time']
        
        self.logger.error("=" * 60)
        self.logger.error("✗ PIPELINE ETL FALLÓ")
        self.logger.error("=" * 60)
        self.logger.error(f"Duración hasta fallo: {duration}")
        self.logger.error(f"Error: {type(error).__name__}: {error}")
        self.logger.error(f"Registros procesados antes del fallo: {self.metrics['processed']}")
        self.logger.error("=" * 60)


# ============================================================================
# EJECUCIÓN Y VERIFICACIÓN
# ============================================================================

if __name__ == "__main__":
    # Ejecutar pipeline
    pipeline = RobustETLPipeline()
    
    try:
        pipeline.run_pipeline()
        
        # Verificar resultados en base de datos
        print("\n" + "=" * 60)
        print("VERIFICACIÓN DE RESULTADOS")
        print("=" * 60)
        
        with sqlite3.connect('etl_database.db') as conn:
            # Contar registros
            count_result = pd.read_sql(
                'SELECT COUNT(*) as registros FROM datos_transformados', 
                conn
            )
            print(f"Total de registros en BD: {count_result.iloc[0, 0]}")
            
            # Mostrar muestra de datos
            sample_result = pd.read_sql(
                'SELECT * FROM datos_transformados LIMIT 5', 
                conn
            )
            print("\nMuestra de datos:")
            print(sample_result)
            
            # Estadísticas por categoría
            stats_result = pd.read_sql('''
                SELECT 
                    categoria_normalizada,
                    COUNT(*) as cantidad,
                    AVG(valor) as valor_promedio,
                    AVG(valor_cuadrado) as valor_cuadrado_promedio
                FROM datos_transformados
                GROUP BY categoria_normalizada
            ''', conn)
            print("\nEstadísticas por categoría:")
            print(stats_result)
        
        print("=" * 60)
        print("✓ Pipeline ejecutado y verificado exitosamente")
        print("✓ Revisa el archivo 'etl_pipeline.log' para ver los logs detallados")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error fatal en el pipeline: {e}")
        print("✓ Revisa el archivo 'etl_pipeline.log' para más detalles")