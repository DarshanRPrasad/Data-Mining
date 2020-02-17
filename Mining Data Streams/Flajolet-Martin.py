import tweepy
from random import randrange

class StreamListener(tweepy.StreamListener):
    def getTagsOfTweet(self,tweet_tags):
        l=[]
        for tag_set in tweet_tags:
            l.append(tag_set["text"])
        return l

    def changeReservoir(self, index, tag_rep):
        tweets_150[index] = tag_rep

    def getTags(self):
        count_dict={}
        for tagList in tweets_150:
            for tag in tagList:
                if tag in count_dict:
                    count_dict[tag] = count_dict[tag] + 1
                else:
                    count_dict[tag] = 1
        return count_dict

    def on_status(self, status):
        tweet_tags = status.entities['hashtags']
        global hashed_tweets
        if len(tweet_tags) > 0:
            tag_rep = self.getTagsOfTweet(tweet_tags)
            hashed_tweets += 1
            if(len(tweets_150) > 150):
                swap_index = randrange(hashed_tweets)
                if swap_index < 150:
                    self.changeReservoir(swap_index, tag_rep)
            else:
                tweets_150.append(tag_rep)
            count_of_all_tags = self.getTags()
            sorted_count = sorted(count_of_all_tags.items(), key=lambda x: (-x[1], x[0]))
            top5 = []
            for i in range (0,5):
                if i < len(sorted_count):
                    top5.append(sorted_count[i])
            print("The number of tweets with tags from the beginning:", str(hashed_tweets))

            highest = sorted_count[0][1]
            count=0

            for each in sorted_count:
                if each[1]!=highest:
                    highest=each[1]
                    count+=1
                    if count>=5:
                        break
                print(each[0]+" : "+str(each[1]))
            print()

            # for tag_count in top5:
            #     print(tag_count[0]+" : "+str(tag_count[1]))
            # print()
            #self.writeToFile(top5)

    def writeToFile(self,top5):
        f = open("output.txt","a")
        f.write("Total tweets with # encountered: "+ str(hashed_tweets)+"\n")
        for element in top5:
            f.write(element[0]+":"+element[1]+"\n")
        f.write("\n")
        f.close()

    def on_error(self, status_code):
        if status_code == 420:
            return False

if __name__ == '__main__':
    tweets_150 = []
    hashed_tweets = 0
    auth = tweepy.OAuthHandler("9AULxV3wvbrjIpTAbvF1xZ5bN", "R4MnX87nMoksVBbefmMWCKYLO93KRxE1rfmSmZZnXshLmaDTrW")
    auth.set_access_token("1200168626747084800-uYnsuVvVlpa7ovcon03szVUNcgLRgA",
                          "zZx0EK1HaXcAB7lAP4IGweduyyhYR6YzJ0m6krGer9O0l")
    api = tweepy.API(auth)
    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=["#"])