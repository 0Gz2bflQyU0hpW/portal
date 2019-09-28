package com.weibo.dip.costing.bean;

import java.util.Date;

/**
 * Created by yurun on 18/4/23.
 */
public class Server {

  private int id;

  public enum Role {
    SUMMON("summon"), KAFKA("kafka"), STREAMING("streaming"), HADOOP("hadoop"), STORM("storm");

    private String name;

    Role(String name) {
      this.name = name;
    }
  }

  private Role role;

  public enum Type {

    SHARE("share"), EXCLUSIVE("exclusive");

    private String name;

    Type(String name) {
      this.name = name;
    }
  }

  private Type type;

  private String productUuid;
  private int servers;
  private Date lastUpdate;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public Role getRole() {
    return role;
  }

  public void setRole(Role role) {
    this.role = role;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public String getProductUuid() {
    return productUuid;
  }

  public void setProductUuid(String productUuid) {
    this.productUuid = productUuid;
  }

  public int getServers() {
    return servers;
  }

  public void setServers(int servers) {
    this.servers = servers;
  }

  public Date getLastUpdate() {
    return lastUpdate;
  }

  public void setLastUpdate(Date lastUpdate) {
    this.lastUpdate = lastUpdate;
  }
  
}
