package splendid.execution.util

import java.io.File
import java.io.FileReader
import java.io.IOException
import java.io.Reader
import java.io.StringReader
import java.util.logging.Level

import org.openrdf.repository.RepositoryException
import org.openrdf.repository.sail.SailRepository
import org.openrdf.rio.RDFFormat
import org.openrdf.rio.RDFParseException
import org.openrdf.sail.memory.MemoryStore
import org.restlet.Component
import org.restlet.data.Protocol
import org.restlet.engine.Engine

import net.fortytwo.sesametools.ldserver.LinkedDataServer
import net.fortytwo.sesametools.ldserver.query.SparqlResource

/**
 * A SPARQL endpoint implementation for testing.
 *
 * @param port the listening port.
 */
class SparqlEndpoint(port: Int) {

  val repo = new SailRepository(new MemoryStore())
  repo.initialize()

  // configure SPARQL endpoint
  val server = new LinkedDataServer(repo.getSail(), "", "")
  val component = new Component();
  component.getServers().add(Protocol.HTTP, port);
  component.getDefaultHost().attach("/sparql", new SparqlResource());
  server.setInboundRoot(component);

  // disable verbose logging in Restlet engine
  Engine.setLogLevel(Level.WARNING)

  // prevent LinkedDataServer (SparqlResource) from logging to System.err
  System.err.close()

  def start(): Unit = {
    server.start()
  }

  def stop(): Unit = {
    server.stop()
    repo.shutDown()
  }

  /**
   * Add RDF data provided in a String to the repository.
   *
   * @param rdfData a String containing the RDF data.
   * @param baseUri the base URI for all relative URIs.
   * @param rdfFormat the format of the RDF data.
   */
  def add(rdfData: String, baseUri: String, rdfFormat: RDFFormat): Unit = {
    add(new StringReader(rdfData), baseUri, rdfFormat)
  }

  /**
   * Add RDF data provided in a file to the repository.
   *
   * @param filename name of the file containing the RDF data.
   * @param rdfFormat the format of the RDF data.
   */
  def load(filename: String, rdfFormat: RDFFormat): Unit = {
    val rdfFile = new File(this.getClass().getResource(filename).getFile());
    add(new FileReader(rdfFile), rdfFile.toURI().toString(), rdfFormat)
  }

  private def add(reader: Reader, baseUri: String, rdfFormat: RDFFormat): Unit = {

    try {
      val con = repo.getConnection()

      try {
        con.add(reader, baseUri, rdfFormat)
      } catch {
        // TODO: handle exceptions
        case e: IOException         => e.printStackTrace()
        case e: RDFParseException   => e.printStackTrace()
        case e: RepositoryException => e.printStackTrace()
      } finally {
        con.close()
      }
    } catch {
      case e: RepositoryException => e.printStackTrace()
    }
  }
}

object SparqlEndpoint {
  val DEFAULT_PORT = 8001
  def apply(port: Int = DEFAULT_PORT): SparqlEndpoint = new SparqlEndpoint(port)
}
