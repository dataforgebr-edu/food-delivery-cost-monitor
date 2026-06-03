from pipeline_config import DEFAULT_LOCAL_DIR, ATHENA_OUTPUT_LOCATION

print(type(ATHENA_OUTPUT_LOCATION), ATHENA_OUTPUT_LOCATION)

teste = str(DEFAULT_LOCAL_DIR).split("/")[-1]

print(f"Variável default= {DEFAULT_LOCAL_DIR}")
print(f"Variável teste = {teste}")