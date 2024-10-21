# Makefile para crear la estructura de carpetas del Proyecto ETL Financiero

.PHONY: all create_structure clean

all: create_structure

create_structure:
	@echo "Creando estructura de carpetas del proyecto..."
	@mkdir -p data
	@mkdir -p src
	@mkdir -p tests
	@mkdir -p config
	@touch data/.gitkeep
	@touch src/.gitkeep
	@touch tests/.gitkeep
	@touch config/.gitkeep
	@echo "Estructura de carpetas creada:"
	@tree -L 2

clean:
	@echo "Eliminando estructura de carpetas..."
	@rm -rf data src tests config

# Nota: El comando 'tree' puede no estar disponible en todos los sistemas.
# Si 'tree' no está instalado, puedes usar 'ls -R' como alternativa.