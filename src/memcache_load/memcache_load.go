package main

import (
	"appsinstalled"
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type options struct {
	idfa, gaid, adid, dvid, pattern, logfile string
	goroutines                               int
}

type parsed_app struct {
	key      string
	value    []byte
	dev_type string
}

type counter struct {
	n int32
}

func (c *counter) Add() {
	atomic.AddInt32(&c.n, 1)
}

func (c *counter) Get() int {
	return int(atomic.LoadInt32(&c.n))
}

func (c *counter) Reset() {
	atomic.SwapInt32(&c.n, 0)
}

var default_opt = options{
	idfa:       "127.0.0.1:33013",
	gaid:       "127.0.0.1:33014",
	adid:       "127.0.0.1:33015",
	dvid:       "127.0.0.1:33016",
	pattern:    "data/appsinstalled/*.tsv.gz",
	logfile:    "",
	goroutines: 2,
}

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

// function for parsing strings to "appsinstalled.UserApps"
func parse_appinstalled(unparsed_lines chan string, errors_counter *counter, device_memc map[string]string, wg *sync.WaitGroup, workers int) {
	var (
		app                                   *appsinstalled.UserApps
		dev_type, dev_id, lat, lon, key, line string
		result, raw_apps                      []string
		memcache_upload_wg                    sync.WaitGroup
		memcache_tube                         chan *parsed_app = make(chan *parsed_app)
		n_apps                                int
		apps                                  []uint32
	)
	defer wg.Done()

	// create workers for upload data to memcache
	for i := 0; i < workers; i++ {
		memcache_upload_wg.Add(1)
		go memcache_upload(memcache_tube, &memcache_upload_wg, errors_counter, device_memc)
	}
	// main cycle of function: get lines from channel, parsing them and move to memcache_tube channel
	for line = range unparsed_lines {
		result = strings.Split(line, "\t")
		dev_type, dev_id, lat, lon, raw_apps = result[0], result[1], result[2], result[3], strings.Split(result[4], ",")
		n_apps = len(raw_apps)
		apps = make([]uint32, n_apps)

		for i, oneapp := range raw_apps {
			oneapp, _ := strconv.ParseInt(oneapp, 10, 32)
			apps[i] = uint32(oneapp)
		}

		Lat, err_lat := strconv.ParseFloat(lat, 64)
		Lon, err_lon := strconv.ParseFloat(lon, 64)
		if err_lat != nil || err_lon != nil {
			logger.Println("Invalid geo coords: " + line)
			errors_counter.Add()
		} else {
			app = &appsinstalled.UserApps{
				Apps: apps,
				Lat:  proto.Float64(Lat),
				Lon:  proto.Float64(Lon),
			}
			key = dev_type + ":" + dev_id
			value, err := proto.Marshal(app)
			if err != nil {
				logger.Println("Marshaling error: ", err)
				errors_counter.Add()
				continue
			}
			// Put parsed line to channel for upload to memcache
			memcache_tube <- &parsed_app{key, value, dev_type}
		}
	}
	// close channel when there is no data for parsing (channel unparsed_lines is closed)
	close(memcache_tube)
	memcache_upload_wg.Wait()
}

// function for upload parsed apps to memcache
func memcache_upload(memcache_tube chan *parsed_app, wg *sync.WaitGroup, error_counter *counter, device_memc map[string]string) {
	defer wg.Done()
	var (
		memc_clients map[string]*memcache.Client = make(map[string]*memcache.Client)
		mc           *memcache.Client
	)
	// creating memcache clients
	for key, value := range device_memc {
		memc_clients[key] = memcache.New(value)
	}
	// loop over channel and put data to memcache
	for app := range memcache_tube {
		mc = memc_clients[app.dev_type]
		err := mc.Set(&memcache.Item{Key: app.key, Value: app.value})
		if err != nil {
			logger.Println("Error while sending app to memcache: ", err)
			error_counter.Add()
		}
	}
}

// This function get filename from channel, reads file line by line and put lines to channel for parsing
func process_file(filename_channel chan string, wg *sync.WaitGroup, device_memc map[string]string, workers int) {
	var (
		apps_count      uint32
		errors_counter  counter
		unparsed_lines  chan string = make(chan string)
		line_parsers_wg sync.WaitGroup
	)
	defer wg.Done()

	// main cycle of function - we are getting filename from channel
	for filename := range filename_channel {
		apps_count = 0
		errors_counter.Reset()
		// create workers for parsing lines from file
		for i := 0; i < workers; i++ {
			line_parsers_wg.Add(1)
			go parse_appinstalled(unparsed_lines, &errors_counter, device_memc, &line_parsers_wg, workers * 2)
		}

		handle, err := os.Open(filename)
		if err != nil {
			logger.Println("File Open:", filename, err)
		}
		defer handle.Close()

		zipReader, err := gzip.NewReader(handle)
		if err != nil {
			logger.Println("New gzip reader:", err)
		}
		defer zipReader.Close()

		scanner := bufio.NewScanner(zipReader)
		scanner.Split(bufio.ScanLines)
		logger.Println("Processing file:", filename)
		for scanner.Scan() {
			apps_count++
			// send unparsed line to channel for future parsing
			unparsed_lines <- scanner.Text()
		}
		logger.Printf("File %s already processed. Errors: %.2f%%\n", filename, 100*float32(errors_counter.Get())/float32(apps_count))
		dot_rename(filename)
	}
	// close channel after read lines from file
	close(unparsed_lines)
	line_parsers_wg.Wait()
}

// function for renaming file
func dot_rename(fpath string) {
	dir, file := filepath.Split(fpath)
	file = "." + file
	os.Rename(fpath, filepath.Join(dir, file))
}

func main() {
	var (
		wg    sync.WaitGroup
		fchan chan string = make(chan string)
	)

	flag.StringVar(&default_opt.idfa, "idfa", default_opt.idfa, "idfa memcache address")
	flag.StringVar(&default_opt.gaid, "gaid", default_opt.gaid, "gaid memcache address")
	flag.StringVar(&default_opt.adid, "adid", default_opt.adid, "adid memcache address")
	flag.StringVar(&default_opt.dvid, "dvid", default_opt.dvid, "dvid memcache address")
	flag.StringVar(&default_opt.pattern, "pattern", default_opt.pattern, "pattern")
	flag.StringVar(&default_opt.logfile, "logfile", default_opt.logfile, "logfile name")
	flag.IntVar(&default_opt.goroutines, "goroutines", default_opt.goroutines, "number of goroutines")
	flag.Parse()

	if default_opt.logfile != "" {
		logoutput, err := os.OpenFile(default_opt.logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("Error opening logfile", err)
		} else {
			logger.SetOutput(logoutput)
			defer logoutput.Close()
		}

	}

	// create dictionary of device types and memcache addressess
	device_memc := map[string]string{
		"idfa": default_opt.idfa,
		"gaid": default_opt.gaid,
		"adid": default_opt.adid,
		"dvid": default_opt.dvid,
	}

	logger.Println("Creating goroutines for file processing")
	for i := 0; i < default_opt.goroutines; i++ {
		wg.Add(1)
		go process_file(fchan, &wg, device_memc, default_opt.goroutines * 2)
	}

	logger.Println("Searching files by pattern. Except hidden files.")
	files, _ := filepath.Glob(default_opt.pattern)
	// move names of found files to channel for processing
	for _, filename := range files {
		if _, file := filepath.Split(filename); !strings.HasPrefix(file, ".") {
			fchan <- filename
		}
	}

	close(fchan)

	wg.Wait()

	logger.Println("Done!")
}
