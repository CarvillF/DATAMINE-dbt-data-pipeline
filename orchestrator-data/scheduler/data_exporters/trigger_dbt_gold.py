from mage_ai.orchestration.triggers.api import trigger_pipeline

trigger_pipeline(
    'dbt_build_gold', # ID del pipeline a ejecutar
    variables={},                  # Variables opcionales
    check_status=True,             # Espera a que termine para seguir
    error_on_failure=True,         # Falla si el hijo falla
)
