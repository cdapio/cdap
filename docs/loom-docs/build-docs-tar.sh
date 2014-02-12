DOC_FOLDER_NAME=loom-1.0-docs

make clean html
cd build
cp -r html/ $DOC_FOLDER_NAME
tar -czf $DOC_FOLDER_NAME.tar.gz $DOC_FOLDER_NAME
rm -r $DOC_FOLDER_NAME