#!/bin/sh

git clone https://github.com/yhcr-samrelins/cy_fdm_builder.git
sudo 
cd cy_fdm_builder
python setup.py bdist_wheel
pip install dist/FDMBuilder-0.1.0-py3-none-any.whl
cp /opt/c2d/cy_fdm_builder/fdm_builder_tutorials /home/jupyter/ -r
chown -R jupyter /home/jupyter/fdm_builder_tutorials
chgrp -R jupyter /home/jupyter/fdm_builder_tutorials