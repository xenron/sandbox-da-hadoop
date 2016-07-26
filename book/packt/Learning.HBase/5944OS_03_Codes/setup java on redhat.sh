sudo -  // login to root for installation
rpm –Uvh  jdk-8-linux-x64.rpm //rpm file name which ever you download
alternatives --install /usr/bin/java java /opt/jdk1.6.0_37/jre/bin/java 20000 //setting Java alternative 
alternatives --config Java // this will output list of Java available here we can select default Java that we installed.
java –version