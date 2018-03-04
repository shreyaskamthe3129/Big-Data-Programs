matrixM = LOAD '$M' USING PigStorage(',') AS (ithMIndex:long , jthMIndex:long , valueM:double);

matrixN = LOAD '$N' USING PigStorage(',') AS (jthNIndex:long , kthNIndex:long , valueN:double);

joinedJIndex = JOIN matrixM BY jthMIndex , matrixN BY jthNIndex ;

joinedProductResult = FOREACH joinedJIndex GENERATE ithMIndex , jthMIndex , kthNIndex , (valueM * valueN) AS productValue ;

sortedFinallResult = ORDER joinedProductResult BY ithMIndex , kthNIndex ;

groupResult = GROUP joinedProductResult by (ithMIndex , kthNIndex);

finalResult = FOREACH groupResult GENERATE FLATTEN(group) AS (ithMIndex , kthNIndex) , SUM(joinedProductResult.productValue);

sortedResult = ORDER finalResult by ithMIndex , kthNIndex ;

STORE sortedResult INTO '$O' USING PigStorage(',');