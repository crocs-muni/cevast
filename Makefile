interpret = python3
trg = cevast/ tests/test_*
max_line_len = 170

install:
	$(interpret) setup.py install

user_install:
	$(interpret) setup.py install --user

test:
	$(interpret) -m unittest discover -s tests -v

check:
	@echo -e "\e[0;32m[1/4]\e[0;35mflake8 --count --select=E9,F63,F7,F82 --show-source --statistics $(trg)$<\e[0m"
	@$(interpret) -m flake8 --count --select=E9,F63,F7,F82 --show-source --statistics $(trg)
	@echo -e "\e[0;32m[2/4]\e[0;35mpylint -E $(trg)$<\e[0m"
	@$(interpret) -m pylint -E $(trg)
	@echo -e "\e[0;32m[3/4]\e[0;35mflake8 --count --exit-zero --max-complexity=10 --max-line-length=$(max_line_len) --statistics $(trg)$<\e[0m"
	@$(interpret) -m flake8 --count --exit-zero --max-complexity=10 --max-line-length=$(max_line_len) --statistics $(trg)
	@echo -e "\e[0;32m[4/4]\e[0;35mpylint -f colorized --max-line-length=$(max_line_len) --fail-under=8 $(trg)$<\e[0m"
	@$(interpret) -m pylint -f colorized --max-line-length=$(max_line_len) --fail-under=8 $(trg)

format:
	$(interpret) -m black -l $(max_line_len) --target-version py36 -S --diff $(trg) | vim -R -

docs:
	pdoc3 --html --force -o docs/ cevast
	rsync -a --remove-source-files docs/cevast/ docs/
	rm -rf docs/cevast/
	git add docs/ -u

clear:
	rm -rf build/
	rm -rf dist/
	rm -rf cevast.egg-info/
	find . -name __pycache__ -type d -exec rm -rv {} +
	find . -name *.pyc -delete

.PHONY: install user_install test check clear docs
