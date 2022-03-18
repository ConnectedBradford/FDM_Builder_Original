#!/bin/sh

pip uninstall FDMBuilder -y
rm -rf /opt/c2d/cy_fdm_builder
git clone https://github.com/yhcr-samrelins/cy_fdm_builder.git /opt/c2d/cy_fdm_builder
cd /opt/c2d/cy_fdm_builder
python setup.py bdist_wheel
pip install dist/FDMBuilder-0.1.0-py3-none-any.whl

