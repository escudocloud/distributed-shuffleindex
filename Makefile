.PHONY: all clean test

VENV := venv

all: test

test: $(VENV)
	@ $(VENV)/bin/py.test

clean:
	@ rm -rf $(VENV)
	@ find . -type f -name "*.py[co]" -delete
	@ find . -type d -name "__pycache__" -delete

$(VENV):
	@ virtualenv $(VENV)
	@ $(VENV)/bin/pip install -r requirements.txt
