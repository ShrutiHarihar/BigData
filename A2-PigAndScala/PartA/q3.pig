businessdata = load 'inputBusiness/business.csv/' as line;
eachbusinessdata = foreach businessdata generate FLATTEN((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)'))as(businessid,address,categories);
reviewdata = load 'inputReview/review.csv/' as line2;
eachreviewdata = foreach reviewdata generate FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(line2,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)'))as(reviewid,userid,businessid,rating);
output = cogroup eachbusinessdata by businessid, eachreviewdata by businessid;  
dump output;
