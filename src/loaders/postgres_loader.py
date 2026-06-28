import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger("etl.loaders.postgres")

class PostgresLoader:
    """
    Carga DataFrames a PostgreSQL con manejo transaccional.
    Usa batch loading para manejar volúmenes grandes sin colapsar memoria.
    """

    BATCH_SIZE = 5_000

    def __init__(self, database_url: str):
        self.engine: Engine = create_engine(
            database_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        logger.info("Motor de base de datos inicializado")

    @contextmanager
    def _get_connection(self):
        """Gestor de contexto para conexiones con commit/rollback automático."""
        with self.engine.begin() as conn:
            yield conn

    def create_run(self, pipeline_name: str) -> int:
        """
        Registra el inicio de una ejecución en pipeline_runs.
        Retorna el run_id generado.
        """
        with self._get_connection() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO warehouse.pipeline_runs
                        (pipeline_name, started_at, status)
                    VALUES
                        (:name, :started_at, 'running')
                    RETURNING run_id
                """),
                {
                    "name": pipeline_name,
                    "started_at": datetime.now(tz=timezone.utc),
                },
            )
            run_id = result.scalar()
        logger.info(f"Pipeline run registrado: run_id={run_id}")
        return run_id

    def finish_run(
        self,
        run_id: int,
        status: str,
        rows_extracted: int = 0,
        rows_loaded: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """Actualiza el estado final de la ejecución."""
        with self._get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE warehouse.pipeline_runs SET
                        finished_at    = :finished_at,
                        status         = :status,
                        rows_extracted = :rows_extracted,
                        rows_loaded    = :rows_loaded,
                        error_message  = :error_message
                    WHERE run_id = :run_id
                """),
                {
                    "run_id": run_id,
                    "finished_at": datetime.now(tz=timezone.utc),
                    "status": status,
                    "rows_extracted": rows_extracted,
                    "rows_loaded": rows_loaded,
                    "error_message": error_message,
                },
            )
        logger.info(f"Run {run_id} finalizado: {status}")

    def load_taxi_trips(
        self, df: pd.DataFrame, run_id: int
    ) -> int:
        """
        Carga viajes de taxi a PostgreSQL en batches.
        Retorna el número de filas insertadas.
        """
        if df.empty:
            logger.warning("DataFrame vacío, nada que cargar")
            return 0

        total_rows = len(df)
        total_batches = (total_rows // self.BATCH_SIZE) + 1
        total_inserted = 0

        logger.info(
            f"Cargando {total_rows:,} filas en "
            f"{total_batches} batches de {self.BATCH_SIZE:,}"
        )

        records = self._prepare_records(df)

        with self._get_connection() as conn:
            for i in range(0, len(records), self.BATCH_SIZE):
                batch = records[i: i + self.BATCH_SIZE]
                batch_num = (i // self.BATCH_SIZE) + 1

                conn.execute(
                    text("""
                        INSERT INTO warehouse.taxi_trips (
                            vendor_id, pickup_datetime, dropoff_datetime,
                            passenger_count, trip_distance_miles,
                            pickup_location_id, dropoff_location_id,
                            payment_type, fare_amount, tip_amount,
                            total_amount, source_file, loaded_at
                        ) VALUES (
                            :vendor_id, :pickup_datetime, :dropoff_datetime,
                            :passenger_count, :trip_distance_miles,
                            :pickup_location_id, :dropoff_location_id,
                            :payment_type, :fare_amount, :tip_amount,
                            :total_amount, :source_file, :loaded_at
                        )
                    """),
                    batch,
                )

                total_inserted += len(batch)
                logger.info(
                    f"Batch {batch_num}/{total_batches}: "
                    f"{total_inserted:,}/{total_rows:,} filas cargadas"
                )

        logger.info(f"Carga completa: {total_inserted:,} filas")
        return total_inserted
    
    @staticmethod
    def _prepare_records(df: pd.DataFrame) -> list[dict]:
        """
        Convierte DataFrame a lista de dicts compatibles con SQLAlchemy.
        Convierte tipos numpy/pandas a tipos nativos de Python.
        """
        records = df.where(pd.notna(df), None).to_dict(orient="records")

        cleaned = []
        for record in records:
            clean_record = {}
            for key, value in record.items():
                if hasattr(value, "item"):
                    value = value.item()
                clean_record[key] = value
            cleaned.append(clean_record)

        return cleaned