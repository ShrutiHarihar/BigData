Build the projects and generate the jar files with name q1.jar, q2.jar, q3.jar, q4.jar for
question 1, 2, 3 and 4 respectively

Commands to run
-------------------------------------
Question 1
hadoop jar q1.jar data.paloalto.categories.UniqueCategories inputBusiness outputQuestion1

Question 2
hadoop jar q2.jar data.top10.business.Top10Business inputReview outputQuestion2

Question 3
hadoop jar q3.jar data.top10.business.Top10Business inputReview inputBusiness intermediateOutput outputQuestion3

Question 4
hadoop jar q4.jar data.stanford.rating.StanfordRating inputReview inputBusiness outputQuestion4




