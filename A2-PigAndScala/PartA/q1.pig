businessData = load 'inputBusiness/business.csv/' as line;
eachBusinessData = foreach businessData generate FLATTEN((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)'))as(businessid,address,categories);   
filterDetail = FILTER eachBusinessData BY(address matches '.*Palo Alto.*');
reviewData = load 'inputReview/review.csv/' as line2;
eachReviewData = foreach reviewData generate FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(line2,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)'))as(reviewid,userid,businessid,rating);
intermediatedata1 = join filterDetail by businessid, eachReviewData by businessid;
intermediatedata2 = GROUP intermediatedata1 by(filterDetail::businessid, filterDetail::address, filterDetail::categories);   
intermediatedata3 = foreach intermediatedata2 generate group,AVG(intermediatedata1.(eachReviewData::rating))as average;
intermediatedata4 = order intermediatedata3 by average desc;
finaloutput = limit intermediatedata4 10;
dump finaloutput;
