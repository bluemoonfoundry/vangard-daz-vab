
import argparse
from managers.postgres_db_manager import main as run_daz_load

def run_update_flow(task_status: dict):
    """Runs the full update flow for DAZ product data, including loading new data from PostgreSQL, updating the SQLite database, and generating embeddings.

    Args:
        task_status (dict): A dictionary to track the status of the task, including keys like 'task_status', 'stage', and 'progress'.
    """

    # Create a dictionary of your predefined arguments
    config_dict = {
        'force': False,
        'all': False,
        'phase': 'all', 
        'limit': None
    }

    # Create a Namespace object by unpacking the dictionary
    args = argparse.Namespace(**config_dict)

    try:
        task_status.update(
            {
                "task_status": 2,
                "stage": "load",
                "progress": "Starting data upload...",
            }
        )

        run_daz_load(args)

        task_status.update(
            {
                "task_status": 0,
                "stage": "finished",
                "progress": "Update process finished successfully.",
            }
        )
    except Exception as e:
        task_status.update(
            {
                "task_status": 0, 
                "progress": f"An error occurred: {e}"
            }
        )
