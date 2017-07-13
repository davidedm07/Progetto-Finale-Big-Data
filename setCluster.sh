sudo mv mongodb-org-3.4.repo /etc/yum.repos.d/
sudo yum update
sudo yum install -y mongodb-org
sudo service mongod start