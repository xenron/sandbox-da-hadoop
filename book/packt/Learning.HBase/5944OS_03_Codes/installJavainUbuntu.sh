sudo apt-get purge openjdk*   #remove open JDK from system/it’s on us whether we leave open JDK or remove it , if there is some component on the machine which requires open JDK then we leave it as such or we can remove it
sudo mkdir -p /usr/local/Java #create a directory for Java installation
sudo cp -r jdk-7u45-linux-x64.tar.gz /usr/local/java	#copy downloaded compressed tar to /usr/local/java directory.
cd /usr/local/java #change directory to Java directory we created
sudo tar -xvzf jdk-7u45-linux-x64.tar.gz #extract compressed file to /usr/local/java
ls   # List out the extracted file it will display jdk or other name according to file downloaded
mv jdk jdk # rename jdk to jdk
JAVA_HOME=/usr/local/java/jdk #create Java home variable so that Java home can be found on the system
PATH=$PATH:$HOME/bin:$JAVA_HOME/bin #adding path of Java home to path variable of the system
JRE_HOME=/usr/local/java/jdk/jre #adding jre home to variable
PATH=$PATH:$HOME/bin:$JRE_HOME/bin #adding jre path to system path
export JAVA_HOME #adding runtime Java home
export JRE_HOME  #adding runtime jre home
export PATH   #update whole path with Java and jre
sudo update-alternatives --install "/usr/bin/java" "java" "/usr/local/java/jdk/jre/bin/java" 1  #this will set an alternative Java available to system
sudo update-alternatives --install "/usr/bin/javac" "javac" "/usr/local/java/jdk/bin/javac" 1  # set javac path
sudo update-alternatives --install "/usr/bin/javaws" "javaws" "/usr/local/java/jdk/jre/bin/javaws" 1  # set javaws path
sudo update-alternatives --set java /usr/local/java/jdk/jre/bin/java  
sudo update-alternatives --set javac /usr/local/java/jdk/bin/javac #set the java runtime environment for the system
sudo update-alternatives --set javaws /usr/local/java/jdk/bin/javaws #set the javac compiler for the system
sudo update-alternatives --set javaws /usr/local/java/jdk/jre/bin/javaws  # set Java Web start for the system
/etc/profile  #this will relode the variable set in profile file
java –version #check if Java is installed properly if it is a success it should give the installed Java version as an output.


