curl -sSL https://bootstrap.pypa.io/get-pip.py -o ~/get-pip.py
python3 ~/get-pip.py
pip3 install virtualenv
virtualenv ~/token_scan_venv
source ~/token_scan_venv/bin/activate
pip3 install -r requirements.txt