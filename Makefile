interpret = python3.6
trg = cevast tests
max_line_len = 127

install:
	$(interpret) setup.py install

user_install:
	$(interpret) setup.py install --user

test:
	$(interpret) -m unittest discover -s tests -v
	rm -rf tests/test_storage

check:
	$(interpret) -m flake8 --count --select=E9,F63,F7,F82 --show-source --statistics $(trg)
	$(interpret) -m pylint -E $(trg)
	$(interpret) -m flake8 --count --exit-zero --max-complexity=10 --max-line-length=$(max_line_len) --statistics $(trg)
	$(interpret) -m pylint -f colorized --max-line-length=$(max_line_len) --fail-under=7 $(trg)

format:
	$(interpret) -m black --target-version py36 -S --diff $(trg) | vim -R -

clear:
	rm -rf build/
	rm -rf dist/
	rm -rf cevast.egg-info/
	find . -name __pycache__ -type d -exec rm -rv {} +

.PHONY: install user_install test check clear
