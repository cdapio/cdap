#!/bin/bash
#!/bin/bash
if [ "$#" -ne 1 ] ; then
     echo "Usage: $0 [hostname]" >&2
     exit 1
fi

customerId=1
productId=1

while :
do
   purchase="1|{\"customer\":\"alex\",\"product\":\"FisherPrice\",\"quantity\":10,\"price\":\"100\",\"purchaseTime\":\"129308132\"}"
   product="2|{\"productId\":\" $productId \",\"description\":\"FisherPrice\"}"
   customer="3|{\"customerId\":\" $customerId \",\"name\":\"alex\",\"zip\":90210,\"rating\":100}"

   echo $purchase
   echo $product
   echo $customer

   i=`curl -q -d "$product" http://$1:10000/stream/transactionStream  #2>&1`
   i=`curl -q -d "$customer" http://$1:10000/stream/transactionStream  #2>&1`
   i=`curl -q -d "$purchase" http://$1:10000/stream/transactionStream  #2>&1`

   customerId++
   productId++
done
