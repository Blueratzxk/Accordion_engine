/**
 * @file connection.h
 * @brief header definitions for restclient-cpp connection class
 * @author Daniel Schauenberg <d@unwiredcouch.com>
 * @version
 * @date 2010-10-11
 */

#ifndef INCLUDE_RESTCLIENT_CPP_CONNECTION_H_
#define INCLUDE_RESTCLIENT_CPP_CONNECTION_H_

#include <curl/curl.h>
#include <string>
#include <map>
#include <cstdlib>

#include "restclient.h"
#include "version.h"

/**
 * @brief namespace for all RestClient definitions
 */
namespace RestClient {

/**
 * @brief define type used for RestClient write callback
 */
typedef size_t (*WriteCallback)(void *data, size_t size,
                  size_t nmemb, void *userdata);

/**
  * @brief Connection object for advanced usage
  */
class Connection {
 public:
    /**
      *  @struct RequestInfo
      *  @brief holds some diagnostics information
      *  about a request
      *  @var RequestInfo::totalTime
      *  Member 'totalTime' contains the total time of the last request in
      *  seconds Total time of previous transfer. See CURLINFO_TOTAL_TIME
      *  @var RequestInfo::nameLookupTime
      *  Member 'nameLookupTime' contains the time spent in DNS lookup in
      *  seconds Time from start until name resolving completed. See
      *  CURLINFO_NAMELOOKUP_TIME
      *  @var RequestInfo::connectTime
      *  Member 'connectTime' contains the time it took until Time from start
      *  until remote host or proxy completed. See CURLINFO_CONNECT_TIME
      *  @var RequestInfo::appConnectTime
      *  Member 'appConnectTime' contains the time from start until SSL/SSH
      *  handshake completed. See CURLINFO_APPCONNECT_TIME
      *  @var RequestInfo::preTransferTime
      *  Member 'preTransferTime' contains the total time from start until
      *  just before the transfer begins. See CURLINFO_PRETRANSFER_TIME
      *  @var RequestInfo::startTransferTime
      *  Member 'startTransferTime' contains the total time from start until
      *  just when the first byte is received. See CURLINFO_STARTTRANSFER_TIME
      *  @var RequestInfo::redirectTime
      *  Member 'redirectTime' contains the total time taken for all redirect
      *  steps before the final transfer. See CURLINFO_REDIRECT_TIME
      *  @var RequestInfo::redirectCount
      *  Member 'redirectCount' contains the number of redirects followed. See
      *  CURLINFO_REDIRECT_COUNT
      *  @var RequestInfo::curlCode
      *  Member 'curlCode' contains the cURL code (cast to int). See
      *  libcurl-errors
      *  @var RequestInfo::curlError
      *  Member 'curlError' contains the cURL error as a string, if any. See
      *  CURLOPT_ERRORBUFFER
      */
    typedef struct {
        double totalTime;
        double nameLookupTime;
        double connectTime;
        double appConnectTime;
        double preTransferTime;
        double startTransferTime;
        double redirectTime;
        // note: libcurl specifies redirectCount is a long, but cpplint
        // won't let us use long to match. So we must default to the
        // largest int type available so as to not allow curl to corrupt
        // the curlCode in that follows in this struct
        uint64_t redirectCount;
        int curlCode;
        std::string curlError;


      } RequestInfo;
    /**
      *  @struct Info
      *  @brief holds some diagnostics information
      *  about the connection object it came from
      *  @var Info::baseUrl
      *  Member 'baseUrl' contains the base URL for the connection object
      *  @var Info::headers
      *  Member 'headers' contains the HeaderFields map
      *  @var Info::timeout
      *  Member 'timeout' contains the configured timeout
      *  @var Info::followRedirects
      *  Member 'followRedirects' contains whether or not to follow redirects
      *  @var Info::maxRedirects
      *  Member 'maxRedirects' contains the maximum number of redirect to follow (-1 unlimited)
      *  @var Info::progressFn
      *  Member 'progressFn' file progress callback function
      *  @var Info::progressFnData
      *  Member 'progressFnData' file progress callback data
      *  @var Info::basicAuth
      *  Member 'basicAuth' contains information about basic auth
      *  @var basicAuth::username
      *  Member 'username' contains the basic auth username
      *  @var basicAuth::password
      *  Member 'password' contains the basic auth password
      *  @var Info::certPath
      *  Member 'certPath' contains the certificate file path
      *  @var Info::certType
      *  Member 'certType' contains the certificate type
      *  @var Info::keyPath
      *  Member 'keyPath' contains the SSL key file path
      *  @var Info::keyPassword
      *  Member 'keyPassword' contains the SSL key password
      *  @var Info::customUserAgent
      *  Member 'customUserAgent' contains the custom user agent
      *  @var Info::uriProxy
      *  Member 'uriProxy' contains the HTTP proxy address
      *  @var Info::lastRequest
      *  Member 'lastRequest' contains metrics about the last request
      */
    typedef struct {
      std::string baseUrl;
      RestClient::HeaderFields headers;
      int timeout;

      bool followRedirects;
      int maxRedirects;
      bool noSignal;
      curl_progress_callback progressFn;
      void* progressFnData;
      struct {
        std::string username;
        std::string password;
      } basicAuth;

      std::string certPath;
      std::string certType;
      std::string keyPath;
      std::string keyPassword;
      std::string customUserAgent;
      std::string uriProxy;
      std::string unixSocketPath;
      RequestInfo lastRequest;
    } Info;

    bool debug = false;
    int timeoutMS = 0;
    FILE *debugFile = NULL;
    explicit Connection(const std::string& baseUrl);
    explicit Connection(const std::string& baseUrl,bool isLong);
    ~Connection();

    // Terminate open connection
    void Terminate();

    // Instance configuration methods
    // configure basic auth
    void SetBasicAuth(const std::string& username,
                      const std::string& password);

    // set connection timeout to seconds
    void SetTimeout(int seconds);
    void SetTimeoutMS(int seconds);
    // set file progress callback
    void SetFileProgressCallback(curl_progress_callback progressFn);

    // set file progress callback data
    void SetFileProgressCallbackData(void* data);

    // set to not use signals
    void SetNoSignal(bool no);

    // set whether to follow redirects, maxRedirects indicitng the maximum
    // number of redirects to follow
    void FollowRedirects(bool follow, int maxRedirects = -1l);

    // set custom user agent
    // (this will result in the UA "foo/cool restclient-cpp/VERSION")
    void SetUserAgent(const std::string& userAgent);

    // set the Certificate Authority (CA) Info which is the path to file holding
    // certificates to be used to verify peers. See CURLOPT_CAINFO
    void SetCAInfoFilePath(const std::string& caInfoFilePath);

    // set CURLOPT_SSLCERT
    void SetCertPath(const std::string& cert);

    // set CURLOPT_SSLCERTTYPE
    void SetCertType(const std::string& type);

    // set CURLOPT_SSLKEY. Default format is PEM
    void SetKeyPath(const std::string& keyPath);

    // set CURLOPT_SSL_VERIFYPEER. Default is true.
    void SetVerifyPeer(bool verifyPeer);

    // set CURLOPT_KEYPASSWD.
    void SetKeyPassword(const std::string& keyPassword);

    // set CURLOPT_PROXY
    void SetProxy(const std::string& uriProxy);

    // set CURLOPT_UNIX_SOCKET_PATH
    void SetUnixSocketPath(const std::string& unixSocketPath);

    // set CURLOPT_WRITEFUNCTION
    void SetWriteFunction(WriteCallback write_callback);

    void setDebug();

    std::string GetUserAgent();

    RestClient::Connection::Info GetInfo();

    // set headers
    void SetHeaders(RestClient::HeaderFields headers);

    // get headers
    RestClient::HeaderFields GetHeaders();

    // append additional headers
    void AppendHeader(const std::string& key,
                      const std::string& value);


    // Basic HTTP verb methods
    RestClient::Response get(const std::string& uri);
    RestClient::Response post(const std::string& uri,
                              const std::string& data);
    RestClient::Response put(const std::string& uri,
                             const std::string& data);
    RestClient::Response patch(const std::string& uri,
                             const std::string& data);
    RestClient::Response del(const std::string& uri);
    RestClient::Response head(const std::string& uri);
    RestClient::Response options(const std::string& uri);

    // GET with custom response structure
    RestClient::Response*
    get(const std::string& uri, RestClient::Response* response);

 private:
    CURL* getCurlHandle();
    CURL* curlHandle;
    std::string baseUrl;
    RestClient::HeaderFields headerFields;
    int timeout;
    bool followRedirects;
    int maxRedirects;
    bool noSignal;
    curl_progress_callback progressFn;
    void* progressFnData;
    struct {
      std::string username;
      std::string password;
    } basicAuth;
    std::string customUserAgent;
    std::string caInfoFilePath;
    RequestInfo lastRequest;
    std::string certPath;
    std::string certType;
    std::string keyPath;
    std::string keyPassword;
    bool verifyPeer;
    std::string uriProxy;
    std::string unixSocketPath;
    char curlErrorBuf[CURL_ERROR_SIZE];
    RestClient::WriteCallback writeCallback;
    RestClient::Response*
    performCurlRequest(const std::string& uri, RestClient::Response* resp);
    RestClient::Response performCurlRequest(const std::string& uri);
};
};  // namespace RestClient

#endif  // INCLUDE_RESTCLIENT_CPP_CONNECTION_H_
