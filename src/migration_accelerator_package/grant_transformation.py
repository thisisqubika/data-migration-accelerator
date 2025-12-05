"""
Entry point for grant flattening transformation.
Runs as a Databricks wheel task using 'grant-transformer'.
"""

import json
from databricks.sdk.runtime import *

from migration_accelerator_package.snowpark_utils import get_uc_volume_path
from migration_accelerator_package.grant_transformer import GrantFlattener


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
    print("=" * 80)
    print(" GRANT FLATTENING TRANSFORMATION ")
    print("=" * 80)

    volume_path = get_uc_volume_path()
    print(f"UC Volume Path: {volume_path}")

    # Initialize flattener
    flattener = GrantFlattener(volume_path)

    # Load artifacts
    print("\n📂 Loading governance artifacts...")
    artifacts = flattener.load_artifacts()
    print(f"✓ Loaded {len(artifacts['roles'])} roles")
    print(f"✓ Loaded {len(artifacts['privileges'])} privilege grants")
    print(f"✓ Loaded {len(artifacts['hierarchy'])} hierarchy relationships")

    # Flatten privileges (hierarchy graph built internally)
    print("\n🔄 Flattening privileges...")
    flattened = flattener.flatten_privileges()
    print(f"✓ Generated {len(flattened)} flattened privilege grants")

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

    print("\n📊 Transformation Statistics:")
    print(json.dumps(stats, indent=2))

    # Save flattened grants
    print("\n💾 Saving flattened grants...")
    metadata = {
        "database": artifacts.get('database'),
        "schema": artifacts.get('schema')
    }
    flattener.save_flattened_grants(flattened, metadata)
    
    output_path = f"{volume_path}/grants_flattened.json"
    print(f"✓ Saved flattened grants to {output_path}")

    print("\n✓ Grant flattening transformation complete!")


if __name__ == "__main__":
    main()