.PHONY: all clean test local remote s3 ecs_s3 ecs_swift

VENV := venv

all: test

test: $(VENV)
	@ $(VENV)/bin/py.test

clean:
	@ rm -rf $(VENV)
	@ find . -type f -name "*.py[co]" -delete
	@ find . -type d -name "__pycache__" -delete

$(VENV): requirements.txt
	@ virtualenv $(VENV)
	@ $(VENV)/bin/pip install -r requirements.txt

local remote s3 ecs_s3 ecs_swift: $(VENV)
	@ $(VENV)/bin/python bench.py $@
