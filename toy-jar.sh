if [ -f build/classes/test/com/continuuity/ToyApp.class ]
then
    rm -r /tmp/toy-jar/*
    mkdir -p /tmp/toy-jar/META-INF
    echo "Manifest-Version: 1.0" >> /tmp/toy-jar/META-INF/MANIFEST.MF
    echo "Main-Class: com.continuuity.ToyApp" >> /tmp/toy-jar/META-INF/MANIFEST.MF
    mkdir -p /tmp/toy-jar/com/continuuity
    cp build/classes/test/com/continuuity/Toy* /tmp/toy-jar/com/continuuity/
    rm /tmp/toy.jar
    cd /tmp/toy-jar && zip -r /tmp/toy.jar ./*; cd -
else
    echo "build/classes/test/com/continuuity/ToyApp.class not found, make sure you compiled app-fabric test sources (or did gradle build)"
fi


