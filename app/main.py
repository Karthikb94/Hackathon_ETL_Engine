import os
import json
import time
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from .logger import get_logger
from .reader import read_parquet_file
from .transformer import apply_transformations
from .writer import write_output
from .exceptions import ETLError, MappingError, TransformError, ValidationError, WriterError
from .utils import timestamp_run_id

app = FastAPI(
    title="ETL Engine (FastAPI + Polars)", 
    version="1.0.0",
    description="A high-performance ETL engine built with FastAPI and Polars",
    docs_url="/docs",
    redoc_url="/redoc"
)

BASE_OUTPUT_DIR = os.environ.get("ETL_OUTPUT_DIR", "output")
BASE_LOGS_DIR = os.environ.get("ETL_LOGS_DIR", "logs")

@app.on_event("startup")
async def startup_event():
    """Ensure required directories exist on startup."""
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    os.makedirs(BASE_LOGS_DIR, exist_ok=True)

@app.get("/health")
async def health_check():
    """Health check endpoint to verify the service is running."""
    return {"status": "healthy", "service": "ETL Engine", "version": "1.0.0"}

@app.post("/transform")
async def transform_endpoint(parquet_file: UploadFile = File(...), mapping_file: UploadFile = File(...)):
    # Input validation
    if not parquet_file.filename or not parquet_file.filename.endswith('.parquet'):
        raise HTTPException(status_code=400, detail="Invalid parquet file. Please upload a .parquet file.")
    
    if not mapping_file.filename or not mapping_file.filename.endswith('.json'):
        raise HTTPException(status_code=400, detail="Invalid mapping file. Please upload a .json file.")
    
    run_id = timestamp_run_id()
    logger, log_path = get_logger(run_id, logs_dir=BASE_LOGS_DIR)
    
    # Start timing
    start_time = time.time()
    logger.info("ETL run started")
    
    run_dir = os.path.join(BASE_OUTPUT_DIR, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)
    parquet_path = os.path.join(run_dir, parquet_file.filename or "input.parquet")
    mapping_path = os.path.join(run_dir, mapping_file.filename or "mapping.json")

    try:
        # Save uploaded files
        file_save_start = time.time()
        with open(parquet_path, "wb") as f:
            f.write(await parquet_file.read())
        with open(mapping_path, "wb") as f:
            f.write(await mapping_file.read())
        file_save_time = (time.time() - file_save_start) * 1000
        logger.info(f"Files saved in {file_save_time:.2f}ms")

        # Load and validate mapping configuration
        mapping_load_start = time.time()
        try:
            with open(mapping_path, "r", encoding="utf-8") as f:
                mapping_cfg = json.load(f)
        except json.JSONDecodeError as e:
            raise MappingError(f"Invalid JSON in mapping file: {e}")
        except Exception as e:
            raise MappingError(f"Failed to read mapping file: {e}")
        
        # Validate mapping configuration
        if not isinstance(mapping_cfg, dict):
            raise MappingError("Mapping configuration must be a JSON object")
        
        output_format = mapping_cfg.get("output_format", "csv").lower()
        if output_format not in ["csv", "json", "json_array", "xlsx", "xml", "positional"]:
            raise MappingError(f"Unsupported output format: {output_format}")
        
        xml_cfg = mapping_cfg.get("xml_config", {})
        output_base = mapping_cfg.get("output_path") or os.path.join(BASE_OUTPUT_DIR, "output")
        base_with_ts = f"{output_base}_{run_id}"
        
        mapping_load_time = (time.time() - mapping_load_start) * 1000
        logger.info(f"Mapping loaded and validated in {mapping_load_time:.2f}ms")

        # Read and validate parquet file
        read_start = time.time()
        df = read_parquet_file(parquet_path)
        if df.height == 0:
            logger.warning("Input parquet file is empty")
        read_time = (time.time() - read_start) * 1000
        logger.info(f"Read parquet: {parquet_path} | rows={df.height}, cols={df.width} | time={read_time:.2f}ms")

        # Validate mappings
        mappings = mapping_cfg.get("mappings", [])
        if not isinstance(mappings, list):
            raise MappingError("'mappings' must be a list")
        if not mappings:
            raise MappingError("No 'mappings' found in mapping.json")

        # Apply transformations
        try:
            transform_start = time.time()
            transformed = apply_transformations(df, mappings)
            transform_time = (time.time() - transform_start) * 1000
            logger.info(f"Transform complete | rows={transformed.height}, cols={transformed.width} | time={transform_time:.2f}ms")
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise TransformError(f"Failed to apply transformations: {e}")

        # Write output
        try:
            write_start = time.time()
            output_path = write_output(transformed, base_with_ts, output_format, xml_cfg)
            write_time = (time.time() - write_start) * 1000
            logger.info(f"Wrote output: {output_path} | time={write_time:.2f}ms")
        except Exception as e:
            logger.error(f"Failed to write output: {e}")
            raise WriterError(f"Failed to write output: {e}")

        # Calculate total time and performance metrics
        total_time = (time.time() - start_time) * 1000
        input_rows = df.height
        output_rows = transformed.height
        throughput = input_rows / (total_time / 1000) if total_time > 0 else 0
        
        logger.info(f"ETL completed successfully in {total_time:.2f}ms")
        logger.info(f"Performance: {throughput:,.0f} rows/second")
        logger.info(f"Data reduction: {input_rows:,} → {output_rows:,} rows ({((input_rows-output_rows)/input_rows*100):.1f}% reduction)")
        
        return {
            "status": "success",
            "run_id": run_id,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "processing_time_ms": round(total_time, 2),
            "throughput_rows_per_sec": round(throughput, 0),
            "output_path": output_path
        }
    except (MappingError, TransformError, ValidationError, WriterError, ETLError) as e:
        logger.error(f"ETL failed: {e}")
        # Clean up temporary files on error
        try:
            if os.path.exists(parquet_path):
                os.remove(parquet_path)
            if os.path.exists(mapping_path):
                os.remove(mapping_path)
        except Exception as cleanup_error:
            logger.warning(f"Failed to cleanup temporary files: {cleanup_error}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        # Clean up temporary files on error
        try:
            if os.path.exists(parquet_path):
                os.remove(parquet_path)
            if os.path.exists(mapping_path):
                os.remove(mapping_path)
        except Exception as cleanup_error:
            logger.warning(f"Failed to cleanup temporary files: {cleanup_error}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")
