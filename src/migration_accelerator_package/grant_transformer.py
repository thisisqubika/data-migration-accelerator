"""
Grant Flattening Transformer
Provides transformation logic for flattening Snowflake role hierarchies
"""
import json
from typing import Dict, List, Any, Set
from databricks.sdk.runtime import dbutils
from migration_accelerator_package.constants import ArtifactType, ArtifactFileName
from migration_accelerator_package.snowpark_utils import load_json_from_volume

class GrantFlattener:
    """
    Flattens Snowflake role hierarchies into direct privilege assignments
    suitable for Databricks Unity Catalog groups.
    """
    
    def __init__(self, volume_path: str):
        """
        Initialize the flattener with Unity Catalog volume path.
        """
        self.volume_path = volume_path
        self.roles = []
        self.privileges = []
        self.hierarchy = []
    
    def load_artifacts(self) -> Dict[str, Any]:
        """
        Load roles, privileges, and hierarchy from JSON files.
        
        Returns:
            Dictionary with loaded artifacts
        """
        roles_data = load_json_from_volume(self.volume_path, "roles.json")
        privileges_data = load_json_from_volume(self.volume_path, "grants_privileges.json")
        hierarchy_data = load_json_from_volume(self.volume_path, "grants_hierarchy.json")

        self.roles = roles_data.get("roles", [])
        self.privileges = privileges_data.get("grants_privileges", [])
        self.hierarchy = hierarchy_data.get("grants_hierarchy", [])

        metadata = {
            "database": roles_data.get("database") or privileges_data.get("database"),
            "schema": roles_data.get("schema") or privileges_data.get("schema"),
        }

        return {
            **metadata,
            "roles": self.roles,
            "privileges": self.privileges,
            "hierarchy": self.hierarchy,
        }    
    def build_hierarchy_graph(self) -> Dict[str, List[str]]:
        """
        Build parent→children mapping from hierarchy grants.
        
        Returns:
            Dictionary mapping each role to its children roles
        """
        graph = {}

        # Ensure every role is present in the graph even if no children
        for role in self.roles:
            name = role.get("name")
            if name:
                graph.setdefault(name, []) #Places node with no children

        # Process hierarchy grants
        for grant in self.hierarchy: #Iterate through each role to role grant `edges`
            parent = grant.get("parent_role")
            child = grant.get("grantee_name")

            # Initialize parent key if missing
            graph.setdefault(parent, [])

            # Add child relationship
            if child not in graph[parent]:
                graph[parent].append(child)

            # Ensure child exists as a key as well
            graph.setdefault(child, [])
        return graph
        
    def collect_direct_privileges(self) -> Dict[str, List[Dict]]:
        """
        Group privileges by role_name.
        
        Returns:
            Dictionary mapping role names to their direct privileges
        """
        direct = {}

        for role in self.roles:
            name = role.get("name")
            if name:
                direct[name] = []

        for grant in self.privileges:
            role_name = grant.get("role_name")

            if role_name not in direct:
                direct[role_name] = []

            direct[role_name].append(grant)

        return direct
    
    def flatten_privileges(self) -> List[Dict[str, Any]]:
        """
        Main flattening logic: traverse hierarchy and accumulate privileges.
        
        Returns:
            List of flattened privilege grants
        """
        direct_privs = self.collect_direct_privileges()
        
        # BUG FIX: hierarchy_graph must be built before use
        hierarchy_graph = self.build_hierarchy_graph()

        flattened = []

        for role in direct_privs.keys():
            visited = set()

            role_privs = self._flatten_role(
                role=role,
                direct_privileges=direct_privs,
                hierarchy_graph=hierarchy_graph,
                visited=visited
            )

            flattened.extend(role_privs)

        return flattened
    
    def _flatten_role(
        self,
        role: str,
        direct_privileges: Dict[str, List[Dict]],
        hierarchy_graph: Dict[str, List[str]],
        visited: Set[str]
    ) -> List[Dict[str, Any]]:
        """
        Recursively flatten privileges for a single role.
        
        Args:
            role: Role name to flatten
            direct_privileges: Map of role→privileges
            hierarchy_graph: Map of parent→children
            visited: Set of visited roles (cycle detection)
        
        Returns:
            List of all privileges (direct + inherited) for this role
        """
        if role in visited:
            print(f"  ⚠ Warning: Circular dependency detected at role '{role}'. Skipping inheritance to prevent loop.")
            return []

        visited.add(role)

        direct = direct_privileges.get(role, [])
        flattened = []

        for p in direct:
            p_copy = p.copy()
            p_copy["source"] = "direct"
            flattened.append(p_copy)

        parent_roles = [
            parent for parent, children in hierarchy_graph.items()
            if role in children
        ]

        for parent in parent_roles:
            if parent not in direct_privileges:
                print(f"  ⚠ Warning: Parent role '{parent}' referenced in hierarchy but missing from roles.json")
                continue

            inherited_privs = self._flatten_role(
                role=parent,
                direct_privileges=direct_privileges,
                hierarchy_graph=hierarchy_graph,
                visited=visited
            )

            for ip in inherited_privs:
                ip_copy = ip.copy()
                # BUG FIX: Update role_name to current role (not parent)
                ip_copy["role_name"] = role
                
                if ip_copy.get("source") == "direct":
                    ip_copy["source"] = f"inherited_from:{parent}"
                elif ip_copy.get("source", "").startswith("inherited_from"):
                    # Keep the original source chain for transitive inheritance
                    pass
                else:
                    ip_copy["source"] = f"inherited_from:{parent}"

                flattened.append(ip_copy)

        flattened = self._deduplicate_privileges(flattened)

        return flattened
    
    def _deduplicate_privileges(self, privileges: List[Dict]) -> List[Dict]:
        """
        Remove duplicate privileges, preferring direct over inherited.
        
        Args:
            privileges: List of privilege grants (may have duplicates)
        
        Returns:
            Deduplicated list of privileges
        """
        # BUG FIX: Use dict to track best privilege per key, preferring direct
        seen = {}

        for p in privileges:
            key = (
                p["role_name"],
                p["privilege"],
                p["granted_on"],
                p["name"]
            )

            if key not in seen:
                seen[key] = p
            elif p.get("source") == "direct":
                # Prefer direct over inherited
                seen[key] = p
            # If existing is direct and new is inherited, keep existing (do nothing)

        return list(seen.values())

    
    def save_flattened_grants(self, flattened: List[Dict], metadata: Dict):
        """
        Save flattened grants to grants_flattened.json.
        
        Args:
            flattened: List of flattened privilege grants
            metadata: Database and schema metadata
        """
        output = {
            "database": metadata.get("database"),
            "schema": metadata.get("schema"),
            "grants_flattened": flattened
        }

        print("✓ Flattening complete")
        print(json.dumps(output, indent=2))

        output_path = f"{self.volume_path}/grants_flattened.json"
        dbutils.fs.put(output_path, json.dumps(output, indent=2), overwrite=True)

        print(f"✓ Flattened grants saved to {output_path}")
