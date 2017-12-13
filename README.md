## MR1 (finding the Average Book Rating)

 > input: BX-Book-Ratings.csv
 Mapper - gets values of ISBN and BookRating and send it in reducer phase, key= ISBN and value= Ratings
 Reducer - get key & iterate it's value = get values and find average.

 ## MR2 (getting most popular ISBN locations) (where are books most popular)

> input: BX-Book-Ratings.csv and BX-Users.csv
 
> Mapper1- Maps values from both the csv file. (by matching the pattern ".*\\d+.*")
 BX-Book-Ratings.csv: get userid, ISBN (key,value)
 BX-User.csv: get userid, location (key, value)
 
 _(one userid can reference multipe ISBNs, but single location)_
 
> Reducer-1 - create new key, from ISBN & userlocation, and its value part will contain user ids
 Mapper-2 - get the key and value, from reducer-1,
 Reducer-2 - get new key (ISBN+userlocation) and iterate its values.
 
 _each iterate will increment the count by 1, (for counting nuber of userids it has)
 (final output will contain ISBN+userlocation & usercounts)_
 
 **this will indicate which ISBN is famous at which location.**

 ## MR3 (how many books each publisher has published every year)

> Mapper - key: yearofpublish + publishername value: 1
 Reducer - get key and iterate through values and sum the count.

 ## MR4 (ISBN vs Age group Analysis) (Which book(ISBN) is most sold in every age-group?)
 
> input: BX-Book-Ratings and BX-Users
 
> Mapper-1 - key: userID value: ISBN
             key: userID value: age
             
_(one user ID can have multiple ISBN but single Age Value)_
 
> Reducer-1 - key: ISBN+agegroup value: 1
  AGEGROUP:
       <pre> if(age <= 19){
    					ageGroup = "Teenager";
    				}
    				else if(age19 && age <=34){
    					ageGroup = "Millenial";
    				}
    				else if(age34 && age <=50){
    					ageGroup = "GenX";
    				}
    				else if(age50 && age <=69){
    					ageGroup = "Boomer";
    				}
    				else{
    					ageGroup = "Silent";
    				}</pre>
                    
> Mapper-2 - key: ISBN+agegroup, value:1
 Reducer-2 - get the key, iterate through values and sum count.

 ## MR5 (How many books have each of these Authors published every year?)

 _same as MR3 but here we are considering authors not publishers._
 
 > input: BX-Books.csv
 
 > Mapper - key: yearofpublish + author, value: 1
 Reducer - get the key and iterate through value and sum count.
 
## MR6 (How many books have been released per year?)

> Mapper - key: year value: 1
 Reducer - get the keys, iterate through values and do sum count.
