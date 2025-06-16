# Legal Data Pipeline - Client Package

## Quick Start

1. **Read the SETUP_GUIDE.md file first** for detailed installation and configuration instructions.

2. Install Python 3.8 or higher and PostgreSQL.

3. Run `install_dependencies.bat` to install required libraries.

4. To customize what data is collected, edit `config/reddit_config.py`.

5. To customize database settings, edit `config.ini`.

6. Run `run_pipeline.bat` to collect and process data.

## Package Contents

- **SETUP_GUIDE.md** - Detailed setup instructions
- **install_dependencies.bat** - Script to install required Python libraries
- **run_pipeline.bat** - Script to run the complete pipeline
- **run_collection_only.bat** - Script to run only data collection
- **run_processing_only.bat** - Script to run only data processing
- **config.ini** - Database configuration settings
- **config/** - Configuration files for different components
- **data_collection/** - Data collection scripts
- **data_pipeline/** - Data processing scripts
