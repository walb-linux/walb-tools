# Setup WalB tutorial environments

## Install virtualbox and vagrant


For Ubuntu,
```
sudo apt install -y virtualbox vagrant
```

For CentOS7,
```
sudo yum install -y virtualbox vagrant
```

## Select Host OS

For Ubuntu16,
```
vagrant box add Ubuntu16 https://cloud-images.ubuntu.com/xenial/current/xenial-server-cloudimg-amd64-vagrant.box
```

For CentOS7,
```
vagrant box add CentOS7 http://cloud.centos.org/centos/7/vagrant/x86_64/images/CentOS-7.box
```

## vagrant up

```
cd walb-tools/misc/vagrant/CentOS7
# cd walb-tools/misc/vagrant/Ubuntu16

vagrant up
```

## Setup WalB


`cd vagrant/CentOS7` or, `cd vagrant/Ubuntu16` .
```
vagrant ssh
./setup.sh
cd walb-tools
sudo ipython
In [1]: execfile("misc/tutorial.py")
```

and type the following commands, and see [tutorial.md](../../../doc/tutorial.md).

