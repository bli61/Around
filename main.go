package main

import (
	elastic "gopkg.in/olivere/elastic.v3"
	"fmt"
	"net/http"
	"encoding/json"
	"log"
	"strconv"
	"reflect"
	"github.com/pborman/uuid"
	"strings"
	"context"
	"cloud.google.com/go/storage"

	//"cloud.google.com/go/bigtable"
	"io"
	//"os"
	"github.com/auth0/go-jwt-middleware"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"
	"github.com/go-redis/redis"
	"time"
)
const (
	INDEX = "around"
	TYPE = "post"
	DISTANCE = "200km"
	PROJECT_ID = "around-187105"
	BT_INSTANCE = "around-post"  //BigTable instance
	ES_URL = "http://35.227.107.152:9200"
	ENABLE_MEMCACHE = true
	BUCKET_NAME = "post-images-187105"
	// Update this with your redis url
	REDIS_URL       = "redis-18863.c1.us-central1-2.gce.cloud.redislabs.com:18863"
)

var mySigningKey = []byte("secret")  //my private key  (server private key)

//type 等于java 中的class，也等于c++中的struc
type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	// `json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User     string `json:"user"`
	Message  string  `json:"message"`
	Location Location `json:"location"`
	Url    string `json:"url"`
}


func main() {
	// Create a client to ElasticSearch
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
                    "mappings":{
 			"post":{
                        	"properties":{
                                         "location":{
                                                "type":"geo_point"
                                         }
                                }
                        }
                    }
             }
             `
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	fmt.Println("started-service") //started ElasticSearch

	// Here we are instantiating the gorilla/mux router
	r := mux.NewRouter()
	var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return mySigningKey, nil
		},
		SigningMethod: jwt.SigningMethodHS256,  //加密方法
	})

	r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
	r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET")
	r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
	r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")
	//logout在client处理，所以不用写

	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))
}


func handlerPost(w http.ResponseWriter, r *http.Request) {
	// Other codes
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	user := r.Context().Value("user")
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"]  //object

	// 32 << 20 is the maxMemory param for ParseMultipartForm, equals to 32MB (1MB = 1024 * 1024 bytes = 2^20 bytes)
	// After you call ParseMultipartForm, the file will be saved in the server memory with maxMemory size.
	// If the file size is larger than maxMemory, the rest of the data will be saved in a system temporary file.
	r.ParseMultipartForm(32 << 20)  //开辟一个很大的内存空间

	// Parse from form data.
	fmt.Printf("Received one post request %s\n", r.FormValue("message"))
	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)
	p := &Post{
		User:    username.(string),  //验证过的username
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}

	id := uuid.New()

	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Image is not available", http.StatusInternalServerError)
		fmt.Printf("Image is not available %v.\n", err)
		return
	}

	ctx := context.Background()
	defer file.Close()  //call this until surrounding codes are finished
	// replace it with your real bucket name.
	_, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v\n", err)
		return
	}
	// Update the media link after saving to GCS.
	p.Url = attrs.MediaLink

	// Save to ES.
	saveToES(p, id)

	// Save to BigTable.
	//saveToBigTable(p, id)
}


func saveToGCS(ctx context.Context, r io.Reader, bucket, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
	// Student questions

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Creates a Bucket instance.
	bucketInstance := client.Bucket(bucket)
	// Next check if the bucket exists
	if _, err = bucketInstance.Attrs(ctx); err != nil {
		return nil, nil, err
	}


	obj := bucketInstance.Object(name)
	w := obj.NewWriter(ctx)

	if _, err = io.Copy(w, r); err != nil {
		return nil, nil, err
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}
	//让所有用户都有read权限
	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, nil, err
	}
	attrs, err := obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)
	return obj, attrs, err
}



//Save a post to ElasticSearch
func saveToES(p *Post, id string) {
	// Create a client, 连上ElasticSearch
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	//Save it to index
	_, err = client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	fmt.Printf("Post is saved to Index: %s\n", p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)

	// range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"  //更新range
	}

	key := r.URL.Query().Get("lat") + ":" + r.URL.Query().Get("lon") + ":" + ran
	if ENABLE_MEMCACHE {
		rs_client := redis.NewClient(&redis.Options{
			Addr:     REDIS_URL,
			Password: "", // no password set
			DB:       0,  // use default DB
		})

		val, err := rs_client.Get(key).Result()
		if err != nil {
			//第一次登录，or缓存失效
			fmt.Printf("Redis cannot find the key %s as %v.\n", key, err)
		} else {
			fmt.Printf("Redis find the key %s.\n", key)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(val))
			return //不用去ElasticSearch找了
		}
	}

	fmt.Printf( "Search received: %f %f %s\n", lat, lon, ran)
	// Create a client, 连上ElasticSearch
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Define geo distance query as specified in
	// https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)  //设定搜索范围

	// Some delay may range from seconds to minutes. So if you don't get enough results. Try it later.
	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from ElasticSearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())
	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization.
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { //相当于 instance of Post in java
		p := item.(Post) // p = (Post) item，强制转换类型
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		// TODO(student homework): Perform filtering based on keywords such as web spam etc.
		if !containsFilteredWords(&p.Message) {
			ps = append(ps, p)  //把 p append到array上
		}
	}
	//convert to json format
	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
		return
	}

	if ENABLE_MEMCACHE {
		//Student question: please complete the Set here
		rs_client := redis.NewClient(&redis.Options{
			Addr:     REDIS_URL,
			Password: "", // no password set
			DB:       0,  // use default DB
		})

		err := rs_client.Set(key, string(js), time.Second*30).Err()
		if err != nil {
			fmt.Printf("Redis cannot save the key %s as %v.\n", key, err)
		}
	}


	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)
}

func containsFilteredWords(s *string) bool {
	filteredWords := []string{
		"fuck",
		"100",
	}
	//check every filteredWords against original string
	for _, word := range filteredWords {
		if strings.Contains(*s, word) {  //s 包含 word
			return true;
		}
	}
	return false;
}




