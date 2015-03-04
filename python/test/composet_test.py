""" cloudstorage compose tester"""
import webapp2
import logging
import random
import re
import cloudstorage
from google.appengine.api import app_identity
from cloudstorage import errors


def alphanum_key(in_string):
  """ Turn a string into a list of string and number chunks.
      "z23a" -> ["z", 23, "a"]
  """
  return [ int(char) if char.isdigit() else char for char in re.split('([0-9]+)', in_string) ]
# pylint: disable=too-few-public-methods
class MainPage(webapp2.RequestHandler):
  """
  Used to test the cloudstorage compose method
  test_sizes_to_run is used to specify how many files to send.
  """
  # pylint: disable=too-many-locals, too-many-statements
  def get(self):
    """
    Main method to start the tests
    """
    test_sizes_to_run = [1, 5, 30, 31, 32, 33, 200, 500, 1000, 1024, 1025]

    bucket_name = app_identity.get_default_gcs_bucket_name()
    final_file_name_template = "/" + bucket_name + "/merged_%i.txt"
    the_list = []
    for item in cloudstorage.listbucket("/%s/input/file" % bucket_name):
      the_list.append(item.filename)

    the_list = sorted(the_list, key=alphanum_key)
    logging.info(the_list)
    the_list = the_list[:max(test_sizes_to_run)]
    for i in range(len(the_list), max(test_sizes_to_run)):
      file_name = "/" + bucket_name + "/input/file%i.txt" % i
      logging.info("Creating File: %s ", file_name)

      with cloudstorage.open(file_name, "w") as gcs_file:
        gcs_file.write("%s\r\n" % str(i))
      the_list.append(file_name)
    list_of_files = []
    for item in the_list:
      logging.info(item)
      list_of_files.append({"file_name" : item.replace("/" + bucket_name + "/", "")})

    list_of_files.sort(key=lambda x: alphanum_key(x['file_name']))

    output = ""
    stats = ""
    preserve_order = True
    random_list = list_of_files[:]
    for test_size in test_sizes_to_run:
      final_file = final_file_name_template % test_size
      stats += "%s: " % final_file
      try:
        if preserve_order:
          random.shuffle(random_list)
          cloudstorage.compose(random_list[:test_size], final_file, preserve_order=preserve_order)
        else:
          cloudstorage.compose(list_of_files[:test_size], final_file, preserve_order=preserve_order)
        try:
          output += final_file + "</p>"
          output += "Sorted: " + str(not preserve_order) + "</p>"
          preserve_order = not preserve_order
          counter = 0
          with cloudstorage.open(final_file, "r") as gcs_source:
            line = gcs_source.readline()
            while line:
              output += line + "</p>"
              counter += 1
              line = gcs_source.readline()
          stats += str(counter) + "</p>"
        except errors.NotFoundError:
          stats += "Error opening file" + "</p>"
          output += "Error opening file"
      except (errors.NotFoundError, ValueError, TypeError) as the_error:
        stats += "Compose threw: %s" % str(the_error) + "</p>"
        output += "Compose threw: %s" % str(the_error) + "</p>"
      output += "********************************************************************</p>"
    self.response.write(stats)
    self.response.write("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@</p>")
    self.response.write(output)

# pylint: disable=invalid-name
app = webapp2.WSGIApplication([
    ('/', MainPage),
], debug=True)




