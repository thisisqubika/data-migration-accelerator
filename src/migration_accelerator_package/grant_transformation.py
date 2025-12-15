"""
Entry point for grant flattening transformation.
Runs as a Databricks wheel task using 'grant-transformer'.
"""

import json
from databricks.sdk.runtime import *

from migration_accelerator_package.snowpark_utils import get_uc_volume_path
from migration_accelerator_package.grant_transformer import GrantFlattener
from migration_accelerator_package.logging_utils import get_app_logger
from migration_accelerator_package.snowpark_utils import get_uc_volume_path
from migration_accelerator_package.grant_transformer import GrantFlattener
from migration_accelerator_package.logging_utils import get_app_logger


logger = get_app_logger("grant-transformer")


def main():
    """
    Main entry point for grant flattening transformation.
    
    Process:
    1. Load extracted governance artifacts
    2. Build role hierarchy graph
    3. Flatten privileges (resolve inheritance)
    4. Save flattened grants
    5. Report statistics
    """
    logger.info("GRANT FLATTENING TRANSFORMATION starting")

    volume_path = get_uc_volume_path()
    logger.info(f"UC Volume Path: {volume_path}")

    # Initialize flattener
    flattener = GrantFlattener(volume_path)

    # Load artifacts
    logger.info("📂 Loading governance artifacts")
    artifacts = flattener.load_artifacts()
    logger.info(f"✓ Loaded {len(artifacts['roles'])} roles")
    logger.info(f"✓ Loaded {len(artifacts['privileges'])} privilege grants")
    logger.info(f"✓ Loaded {len(artifacts['hierarchy'])} hierarchy relationships")

    # Flatten privileges (hierarchy graph built internally)
    logger.info("🔄 Flattening privileges")
    flattened = flattener.flatten_privileges()
    logger.info(f"✓ Generated {len(flattened)} flattened privilege grants")

    # Calculate statistics
    direct_count = sum(1 for p in flattened if p['source'] == 'direct')
    inherited_count = len(flattened) - direct_count
    
    stats = {
        "total_roles": len(artifacts['roles']),
        "total_flattened_grants": len(flattened),
        "direct_grants": direct_count,
        "inherited_grants": inherited_count,
        "expansion_ratio": round(len(flattened) / len(artifacts['privileges']), 2) if artifacts['privileges'] else 0
    }

    logger.info("📊 Transformation Statistics")
    logger.info(json.dumps(stats, indent=2))

    # Save flattened grants
    logger.info("💾 Saving flattened grants")
    metadata = {
        "database": artifacts.get('database'),
        "schema": artifacts.get('schema')
    }
    flattener.save_flattened_grants(flattened, metadata)
    
    output_path = f"{volume_path}/grants_flattened.json"
    logger.info(f"✓ Saved flattened grants to {output_path}")

    logger.info("✓ Grant flattening transformation complete")


if __name__ == "__main__":
    main()