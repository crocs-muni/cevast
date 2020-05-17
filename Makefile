interpret = python3.6
trg = cevast/utils cevast/dataset/parser cevast/certdb
max_line_len = 127

install:
	$(interpret) setup.py install

user_install:
	$(interpret) setup.py install --user

test:
	$(interpret) -m unittest discover -s tests -v
	rm -rf tests/test_storage

check:
	@echo -e "\e[0;32m[1/4]\e[0;35mflake8 --count --select=E9,F63,F7,F82 --show-source --statistics $(trg)$<\e[0m"
	@$(interpret) -m flake8 --count --select=E9,F63,F7,F82 --show-source --statistics $(trg)
	@echo -e "\e[0;32m[2/4]\e[0;35mpylint -E $(trg)$<\e[0m"
	@$(interpret) -m pylint -E $(trg)
	@echo -e "\e[0;32m[3/4]\e[0;35mflake8 --count --exit-zero --max-complexity=10 --max-line-length=$(max_line_len) --statistics $(trg)$<\e[0m"
	@$(interpret) -m flake8 --count --exit-zero --max-complexity=10 --max-line-length=$(max_line_len) --statistics $(trg)
	@echo -e "\e[0;32m[4/4]\e[0;35mpylint -f colorized --max-line-length=$(max_line_len) --fail-under=7 $(trg)$<\e[0m"
	@$(interpret) -m pylint -f colorized --max-line-length=$(max_line_len) --fail-under=7 $(trg)

format:
	$(interpret) -m black -l $(max_line_len) --target-version py36 -S --diff $(trg) | vim -R -

clear:
	rm -rf build/
	rm -rf dist/
	rm -rf cevast.egg-info/
	find . -name __pycache__ -type d -exec rm -rv {} +

.PHONY: install user_install test check clear
