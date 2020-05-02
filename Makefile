install:
	python3.6 setup.py install

user_install:
	python3.6 setup.py install --user

clear:
	rm -rf build/
	rm -rf dist/
	rm -rf cevast.egg-info/

.PHONY: install user_install
