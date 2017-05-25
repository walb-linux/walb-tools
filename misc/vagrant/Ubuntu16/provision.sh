# install develop tools
sudo apt upgrade -y
sudo apt update -y
sudo apt install -y linux-headers-`uname -r` libaio-dev libsnappy-dev liblzma-dev zlib1g-dev python make gcc g++ ipython

# download setup script
#BASE_URL=https://github.dev.cybozu.co.jp/raw/herumi/walb-tools/master/
BASE_URL=https://raw.githubusercontent.com/walb-linux/walb-tools/master/
wget ${BASE_URL}misc/vagrant/setup.sh
chmod +x setup.sh
sudo reboot
