import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger("etl.transformers.taxi")


class TaxiTransformer:
    """
    Transforma datos crudos de NYC Taxi al esquema de destino.
    Responsabilidades:
      1. Renombrar columnas al esquema de destino
      2. Corregir tipos de datos
      3. Filtrar registros inválidos
      4. Agregar columnas de auditoría
    """

    COLUMN_RENAME_MAP = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance_miles",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "tip_amount": "tip_amount",
        "total_amount": "total_amount",
    }

    MIN_TRIP_DISTANCE = 0.1
    MAX_TRIP_DISTANCE = 100.0
    MIN_FARE = 2.50
    MAX_FARE = 500.0

    def transform(self, df: pd.DataFrame, source_file: str = "unknown") -> pd.DataFrame:
        """
        Aplica la cadena completa de transformaciones.
        Retorna el DataFrame limpio listo para cargar a PostgreSQL.
        """
        initial_rows = len(df)
        logger.info(f"Iniciando transformación: {initial_rows:,} filas")

        df = (
            df.pipe(self._rename_columns)
              .pipe(self._fix_dtypes)
              .pipe(self._filter_invalid_records)
              .pipe(self._add_audit_columns, source_file=source_file)
        )

        final_rows = len(df)
        dropped = initial_rows - final_rows
        logger.info(
            f"Transformación completa: {final_rows:,} filas válidas | "
            f"{dropped:,} descartadas ({dropped/initial_rows*100:.1f}%)"
        )
        return df
    
    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renombra columnas al esquema de destino."""
        missing = set(self.COLUMN_RENAME_MAP.keys()) - set(df.columns)
        if missing:
            raise ValueError(f"Columnas faltantes en el dataset: {missing}")
        return df.rename(columns=self.COLUMN_RENAME_MAP)

    def _fix_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Corrige tipos de datos para consistencia y eficiencia de memoria."""
        df = df.copy()

        for col in ["pickup_datetime", "dropoff_datetime"]:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        int_cols = [
            "vendor_id", "passenger_count", "payment_type",
            "pickup_location_id", "dropoff_location_id"
        ]
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int16")

        float_cols = ["trip_distance_miles", "fare_amount", "tip_amount", "total_amount"]
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

        return df
    
    def _filter_invalid_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra registros inválidos con logging detallado por criterio.
        Cada filtro loguea cuántos registros descarta y por qué.
        """
        def apply_filter(df, mask, reason):
            before = len(df)
            df = df[mask]
            dropped = before - len(df)
            if dropped > 0:
                logger.debug(
                    f"Filtro '{reason}': {dropped:,} registros descartados"
                )
            return df

        # Fechas válidas
        mask_dates = (
            df["pickup_datetime"].notna() &
            df["dropoff_datetime"].notna()
        )
        df = apply_filter(df, mask_dates, "fechas nulas")

        # Duración positiva
        mask_duration = df["dropoff_datetime"] > df["pickup_datetime"]
        df = apply_filter(df, mask_duration, "duración inválida")

        # Distancia en rango válido
        mask_distance = df["trip_distance_miles"].between(
            self.MIN_TRIP_DISTANCE,
            self.MAX_TRIP_DISTANCE,
        )
        df = apply_filter(df, mask_distance, "distancia inválida")

        # Tarifa en rango válido
        mask_fare = df["fare_amount"].between(
            self.MIN_FARE,
            self.MAX_FARE,
        )
        df = apply_filter(df, mask_fare, "tarifa inválida")

        return df.reset_index(drop=True)
    
    def _add_audit_columns(
        self, df: pd.DataFrame, source_file: str
    ) -> pd.DataFrame:
        """
        Agrega columnas de auditoría para trazabilidad.
        Permite saber de dónde vino cada registro y cuándo fue cargado.
        """
        df = df.copy()
        df["source_file"] = source_file
        df["loaded_at"] = datetime.now(tz=timezone.utc)
        return df