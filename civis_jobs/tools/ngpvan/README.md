# NGPVAN Pipelines

These pipelines were designed to push information of various kinds collected from other sources into Votebuilder. They are all built around Civis's [Export Civis Data to NGPVAN](https://civis.zendesk.com/hc/en-us/articles/360030805971) functionality, which is a layer on top of [VAN's API](https://developers.ngpvan.com/van-api). 

## Organization

We have provided a few sample projects here to illustrate different uses - one exports survey questions and responses, one exports notes, and one and one creates new person records in MyCampaign. They all have similar structure: 

1) A python runner file named something like `load_<type>.py`. This file was executed to run the pipeline.
2) A `views` directory containing a series of SQL files represting the data to be loaded.
3) Other `prep` and `post_load` SQL files to do some basic logging.

Note that these projects were all developed at different times for slightly different purposes. There is certainly room to improve and standardize code between the different projects - we leave that to future users.
