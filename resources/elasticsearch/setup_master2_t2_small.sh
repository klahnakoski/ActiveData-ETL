
# FOR AMAZON AMI ONLY
# ENSURE THE EC2 INSTANCE IS GIVEN A ROLE THAT ALLOWS IT ACCESS TO S3 AND DISCOVERY
# THIS EXAMPLE WORKS, BUT YOU MAY FIND IT TOO PERMISSIVE
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "NotAction": "iam:*",
#       "Resource": "*"
#     }
#   ]
# }


# NOTE: NODE DISCOVERY WILL ONLY WORK IF PORT 9300 IS OPEN BETWEEN THEM

sudo yum -y update



#INCREASE FILE LIMITS
sudo sed -i '$ a\fs.file-max = 100000' /etc/sysctl.conf
sudo sed -i '$ a\vm.max_map_count = 262144' /etc/sysctl.conf

sudo sed -i '$ a\root soft nofile 100000' /etc/security/limits.conf
sudo sed -i '$ a\root hard nofile 100000' /etc/security/limits.conf
sudo sed -i '$ a\root soft memlock unlimited' /etc/security/limits.conf
sudo sed -i '$ a\root hard memlock unlimited' /etc/security/limits.conf

sudo sed -i '$ a\ec2-user soft nofile 100000' /etc/security/limits.conf
sudo sed -i '$ a\ec2-user hard nofile 100000' /etc/security/limits.conf
sudo sed -i '$ a\ec2-user soft memlock unlimited' /etc/security/limits.conf
sudo sed -i '$ a\ec2-user hard memlock unlimited' /etc/security/limits.conf

#HAVE CHANGES TAKE EFFECT
sudo sysctl -p
sudo su ec2-user

# INSTALL JAVA 8
sudo rpm -i jre-8u201-linux-x64.rpm
sudo alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000
export JAVA_HOME=/usr/java/default

#CHECK IT IS 1.8
java -version

# INSTALL ELASTICSEARCH
cd /home/ec2-user/
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.5.4.tar.gz
tar zxfv elasticsearch-6.5.4.tar.gz
sudo mkdir /usr/local/elasticsearch
sudo cp -R elasticsearch-6.5.4/* /usr/local/elasticsearch/
rm -fr elasticsearch*


# INSTALL CLOUD PLUGIN
cd /usr/local/elasticsearch/
sudo bin/elasticsearch-plugin install -b discovery-ec2

sudo rm -f /usr/local/elasticsearch/config/elasticsearch.yml
sudo rm -f /usr/local/elasticsearch/config/jvm.options
sudo rm -f /usr/local/elasticsearch/config/log4j2.properties


# INSTALL GIT
sudo yum install -y git-core

# INSTALL SUPERVISOR
sudo easy_install pip
sudo pip install supervisor


# SIMPLE PLACE FOR LOGS
sudo mkdir /data1
sudo chown ec2-user:ec2-user -R /data1
mkdir /data1/logs
mkdir /data1/heapdump

ln -s  /data1/logs /home/ec2-user/logs

# CLONE ActiveData-ETL
cd ~
git clone https://github.com/mozilla/ActiveData-ETL.git
git checkout dev
git pull origin dev

###############################################################################
# PLACE ALL CONFIG FILES
###############################################################################

# ELASTICSEARCH CONFIG
sudo chown -R ec2-user:ec2-user /usr/local/elasticsearch
cp ~/ActiveData-ETL/resources/elasticsearch/elasticsearch6_master2.yml     /usr/local/elasticsearch/config/elasticsearch.yml
cp ~/ActiveData-ETL/resources/elasticsearch/jvm_master.options             /usr/local/elasticsearch/config/jvm.options
cp ~/ActiveData-ETL/resources/elasticsearch/log4j2.properties              /usr/local/elasticsearch/config/log4j2.properties

# SUPERVISOR CONFIG
sudo cp ~/ActiveData-ETL/resources/elasticsearch/supervisord.conf /etc/supervisord.conf

# START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
sudo /usr/bin/supervisord -c /etc/supervisord.conf

# READ CONFIG
sudo /usr/bin/supervisorctl reread
sudo /usr/bin/supervisorctl update

sudo supervisorctl


