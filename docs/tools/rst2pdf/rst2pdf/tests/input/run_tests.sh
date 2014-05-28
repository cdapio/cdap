#!/bin/sh

compare_pdfinfo() {
    # Given two PDF files $1 and $2, compare the output of pdfinfo except for 
    # creation date.
    pdfinfo $1 | grep -v CreationDate | grep -v "File size"> $1.info
    pdfinfo $2 | grep -v CreationDate | grep -v "File size"> $2.info
    if cmp -s $1.info $2.info 
    then
	true
        #echo "PDF metadata is correct"
    else
        echo "PDF metadata for $1 is wrong"
    fi
    #rm $1.info $2.info
}

run_test() {
    bn=`basename $1 .txt`
    st="$bn.style"
	if [ -f "$st" ]
    then
        style="-s $st"
    else
	style=""
    fi
    echo "Processing $1"
    
    if python ../../createpdf.py -v $1 $style $RSTOPTIONS 2> $1.err
    then
	if [ -f "correct/$bn.pdf" ]
	then
        	compare_pdfinfo $bn.pdf correct/$bn.pdf
        	if [ ! -d temp-$bn ]
       		then
            		mkdir temp-$bn
        	fi
        	rm -f temp-$bn/*
        	convert $bn.pdf temp-$bn/page.png
        	convert correct/$bn.pdf temp-$bn/correctpage.png
        	cd temp-$bn
        	for page in page*png
        	do
            		result=`compare -metric PSNR $page correct$page diff$page 2>&1`
            		if [ "$result" = "inf" ]
            		then
                		#echo "$page is OK"
				true
            		else
                		echo "$page has ERRORs, see temp-$bn/diff$page"
            		fi
        	done
        	cd ..
	fi	
    else
        echo ERROR processing $1, see $1.err
    fi
}

if [ $# -eq 0 ]
then
	for t in *.txt
	do
		run_test $t
	done
else
	while [ ! -z $1 ]
	do
		run_test $1
		shift
	done

fi
