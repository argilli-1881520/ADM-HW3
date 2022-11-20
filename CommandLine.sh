#Make sure to execute this file in the same folder in which the dataset is contained
#In our case the dataset name is result.tsv 

#This code checks if the address (contained in the column number 8 of our dataset)
#contains the stings States, Italy, Spain, England and France
#and prints only the places which fit with the conditions.
echo 'The number of places in the United States is: '
awk -F '\t' '{if($8 ~ /States/){print $8}}' result.tsv | wc -l
echo 'The number of places in Italy is: '
awk -F '\t' '{if($8 ~ /Italy/){print $8}}' result.tsv | wc -l
echo 'The number of places in Spain is: '
awk -F '\t' '{if($8 ~ /Spain/){print $8}}' result.tsv | wc -l
echo 'The number of places in England is: '
awk -F '\t' '{if($8 ~ /England/){print $8}}' result.tsv | wc -l
echo 'The number of places in France is: '
awk -F '\t' '{if($8 ~ /France/){print $8}}' result.tsv | wc -l

#This code takes the column number 3 of our dataset, which contains the number of people that visited
#places in the taken countries, than saves the row which fit with this condition in a new dataset named *state_name*.tsv.
#After that it sums all the number of people which have visited places in the desired country and than computes the average by
#dividing the sum by the number of raws of the new created dataset. 
echo 'The average number of visitors in the United States is: '
awk -F '\t' '{if($8 ~ /States/){print $3}}' result.tsv >> states.tsv | awk -F '\t' '{sum+= $1} END {print sum/FNR}' states.tsv
echo 'The average number of visitors in the Italy is: '
awk -F '\t' '{if($8 ~ /Italy/){print $3}}' result.tsv >> italy.tsv | awk -F '\t' '{sum+= $1} END {print sum/FNR}' italy.tsv
echo 'The average number of visitors in the Spain is: '
awk -F '\t' '{if($8 ~ /Spain/){print $3}}' result.tsv >> spain.tsv | awk -F '\t' '{sum+= $1} END {print sum/FNR}' spain.tsv
echo 'The average number of visitors in the England is: '
awk -F '\t' '{if($8 ~ /England/){print $3}}' result.tsv >> england.tsv | awk -F '\t' '{sum+= $1} END {print sum/FNR}' england.tsv
echo 'The average number of visitors in the France is: '
awk -F '\t' '{if($8 ~ /France/){print $3}}' result.tsv >> france.tsv | awk -F '\t' '{sum+= $1} END {print sum/FNR}' france.tsv

#This code checks the column number 4 of our dataset, which contains the number of people
#who wants to visit places in the desired country. Than compute the total number by summing every 
#raw which fits with the condtion.
echo 'The total number of people who wants to visit United States is: '
awk -F '\t' '{if ($8 ~ /States/){sum+=$4}} END {print sum}' result.tsv
echo 'The total number of people who wants to visit Italy is: '
awk -F '\t' '{if ($8 ~ /Italy/){sum+=$4}} END {print sum}' result.tsv
echo 'The total number of people who wants to visit Spain is: '
awk -F '\t' '{if ($8 ~ /Spain/){sum+=$4}} END {print sum}' result.tsv
echo 'The total number of people who wants to visit England is: '
awk -F '\t' '{if ($8 ~ /England/){sum+=$4}} END {print sum}' result.tsv
echo 'The total number of people who wants to visit France is: '
awk -F '\t' '{if ($8 ~ /France/){sum+=$4}} END {print sum}' result.tsv