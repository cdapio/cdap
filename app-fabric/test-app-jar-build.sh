APP_NAME=$1

echo Building jar from classes in build/classes/test/com/continuuity/$APP_NAME*.class

if [ -f build/classes/test/com/continuuity/$APP_NAME.class ]
then
    rm -r /tmp/$APP_NAME-jar/*
    mkdir -p /tmp/$APP_NAME-jar/META-INF
    echo "Manifest-Version: 1.0" >> /tmp/$APP_NAME-jar/META-INF/MANIFEST.MF
    echo "Main-Class: com.continuuity.$APP_NAME" >> /tmp/$APP_NAME-jar/META-INF/MANIFEST.MF
    mkdir -p /tmp/$APP_NAME-jar/com/continuuity
    cp build/classes/test/com/continuuity/$APP_NAME* /tmp/$APP_NAME-jar/com/continuuity/
    rm /tmp/$APP_NAME.jar
    cd /tmp/$APP_NAME-jar && zip -r /tmp/$APP_NAME.jar ./*; cd -
    echo
    echo "jar file path: /tmp/$APP_NAME.jar"
else
    echo "build/classes/test/com/continuuity/$APP_NAME.class not found, make sure you compiled app-fabric test sources (or did gradle build)"
fi


