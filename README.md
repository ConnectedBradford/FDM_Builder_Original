### sh script to install FDMBuilder library:

```
git clone https://github.com/ConnectedBradford/cy_fdm_builder.git /home/jupyter/cy_fdm_builder
cd /home/jupyter/cy_fdm_builder/
python setup.py bdist_wheel
pip install dist/FDMBuilder-0.1.0-py3-none-any.whl
```
