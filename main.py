def main():
    from pathlib import Path

    from src.api_client import AeroDataBoxClient
    from src.config import PipelineConfig
    from src.database import SQLiteManager
    from src.etl import FlightETLPipeline, FlightFileStore, FlightTransformer
    from src.logger_config import setup_logging

    base_dir = Path(__file__).resolve().parent
    config = PipelineConfig.default(base_dir)
    logger = setup_logging(config.log_path)

    client = AeroDataBoxClient(logger=logger)
    file_store = FlightFileStore(
        raw_dir=config.raw_dir,
        processed_dir=config.processed_dir,
        reports_dir=config.reports_dir,
        logger=logger,
    )
    transformer = FlightTransformer(logger=logger)
    database_manager = SQLiteManager(db_path=config.db_path, logger=logger)
    pipeline = FlightETLPipeline(
        config=config,
        client=client,
        file_store=file_store,
        transformer=transformer,
        database_manager=database_manager,
        logger=logger,
    )

    pipeline.run()


if __name__ == "__main__":
    main()
