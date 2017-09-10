businessdata = load 'inputBusiness/business.csv/' as line;
eachbusinessdata = foreach businessdata generate FLATTEN((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)'))as(businessid,address,categories);
filterbusinessdata = FILTER eachbusinessdata BY(address matches '.*Stanford.*');
reviewdata = load 'inputReview/review.csv/' as line2;
eachreviewdata = foreach reviewdata generate FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(line2,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)'))as(reviewid,userid,businessid,rating);
intermediatedata1 = join filterbusinessdata by businessid, eachreviewdata by businessid;  
intermediatedata2 = foreach intermediatedata1 generate userid,rating;
finaloutput = limit intermediatedata2 10;
dump finaloutput;
