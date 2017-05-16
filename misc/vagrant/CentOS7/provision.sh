# install develop tools
sudo yum -y update
sudo yum install -y git gcc gcc-c++ kernel-devel snappy-devel xz-devel libaio-devel psmisc binutils-devel wget python-devel
# install jupyter, pylint
wget https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py
sudo pip install jupyter
sudo pip install pylint

# download setup script
BASE_URL=https://github.dev.cybozu.co.jp/raw/herumi/walb-tools/master/
wget ${BASE_URL}misc/vagrant/setup.sh
chmod +x setup.sh
sudo reboot

